#!/usr/bin/env python3
DESC = """
Generate tiny Illumina and ONT test datasets from reference genomes using InSilicoSeq and NanoSim.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, List
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import shutil
from Bio import SeqIO
import os

###########
# LOGGING #
###########

class UTCFormatter(logging.Formatter):
    """Custom logging formatter that displays timestamps in UTC."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        """Format log timestamps in UTC timezone."""
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = UTCFormatter("[%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)

########################
# READ FILE GENERATION #
########################

NANOSIM_MODEL_URL = "https://raw.githubusercontent.com/bcgsc/NanoSim/master/pre-trained_models/human_giab_hg002_sub1M_kitv14_dorado_v3.2.1.tar.gz"

def modify_fasta_headers(input_fasta: Path, output_fasta: Path, suffix: str) -> None:
    """
    Create a copy of a FASTA file with modified headers using Biopython.

    Args:
        input_fasta (Path): Input FASTA file
        output_fasta (Path): Output FASTA file with modified headers
        suffix (str): Suffix to append to each sequence ID
    """
    records = []
    for record in SeqIO.parse(input_fasta, "fasta"):
        record.id = record.id + suffix
        record.description = record.description + suffix
        records.append(record)
    SeqIO.write(records, output_fasta, "fasta")

def generate_illumina(fasta_file: Path,
                   output_prefix: Path,
                   n_read_pairs: int,
                   seed: int,
                   fragment_length: Optional[int] = None,
                   model: str = "NovaSeq") -> Tuple[Path, Path]:
    """
    Generate paired-end Illumina reads from a FASTA file using InSilicoSeq.
    Args:
        fasta_file (Path): Input FASTA file
        output_prefix (Path): Output prefix for generated reads
        n_read_pairs (int): Number of read pairs to generate
        seed (int): Seed for random number generators
        fragment_length (Optional[int]): Fragment length for paired-end reads (default: None)
        model (str): Sequencing error model (default: NovaSeq)
    Returns:
        Tuple[Path, Path]: Paths to R1 and R2 FASTQ files
    """
    logger.info(f"Generating {n_read_pairs} read pairs from {fasta_file.name}...")
    cmd = [
        "iss", "generate",
        "--genomes", str(fasta_file),
        "--n_reads", str(n_read_pairs * 2),
        "--model", model,
        "--output", str(output_prefix),
        "--seed", str(seed),
    ]
    if fragment_length is not None:
        cmd.extend(["--fragment-length", str(fragment_length),
                    "--fragment-length-sd", "0"])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Failed to generate reads from {fasta_file}")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)
    r1_file = Path(f"{output_prefix}_R1.fastq")
    r2_file = Path(f"{output_prefix}_R2.fastq")
    if not r1_file.exists() or not r2_file.exists():
        raise FileNotFoundError(f"Expected output files not found: {r1_file}, {r2_file}")
    logger.info(f"  Generated {r1_file.name} and {r2_file.name}")
    return r1_file, r2_file

def download_nanosim_model(tmpdir: Path, model_url: str) -> Path:
    """
    Download and extract NanoSim statistical model.

    Args:
        tmpdir (Path): Temporary directory for downloads
        model_url (str): URL to the NanoSim model tarball

    Returns:
        Path: Path to the extracted model directory
    """
    model_tarball = tmpdir / "nanosim_model.tar.gz"
    model_dir = tmpdir / "nanosim_model"
    logger.info(f"Downloading NanoSim model from {model_url}...")
    result = subprocess.run([
        "wget", model_url, "-O", str(model_tarball)
    ], capture_output=True, text=True)

    if result.returncode != 0:
        logger.error("Failed to download NanoSim model")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)

    logger.info("Extracting NanoSim model...")
    model_dir.mkdir(exist_ok=True)
    result = subprocess.run([
        "tar", "-xzf", str(model_tarball), "-C", str(model_dir), "--strip-components=1"
    ], capture_output=True, text=True)

    if result.returncode != 0:
        logger.error("Failed to extract NanoSim model")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)

    # Clean up tarball
    model_tarball.unlink()

    return model_dir

def generate_ont(fasta_file: Path,
                 output_prefix: Path,
                 n_reads: int,
                 model_dir: Path,
                 seed: int = 0) -> Path:
    """
    Generate ONT reads from a FASTA file using NanoSim.

    Args:
        fasta_file (Path): Input FASTA file
        output_prefix (Path): Output prefix for generated reads
        n_reads (int): Number of reads to generate
        model_dir (Path): Path to NanoSim model directory
        seed (int): Random seed (default: 0)

    Returns:
        Path: Path to output FASTQ file
    """
    logger.info(f"Generating {n_reads} ONT reads from {fasta_file.name}...")

    # NanoSim expects the model path to point to the training prefix
    model_prefix = model_dir / "training"

    result = subprocess.run([
        "simulator.py", "genome",
        "-rg", str(fasta_file.resolve()),  # Use absolute path
        "-n", str(n_reads),
        "-s", "0.5",  # Strandedness (0.5 = equal forward/reverse)
        "-c", str(model_prefix),
        "-o", str(output_prefix),
        "--seed", str(seed),
        "--fastq"
    ], capture_output=True, text=True, cwd=output_prefix.parent)

    if result.returncode != 0:
        logger.error(f"Failed to generate ONT reads from {fasta_file}")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)

    # NanoSim creates files with specific suffixes
    fastq_file = Path(f"{output_prefix}_aligned_reads.fastq")

    if not fastq_file.exists():
        msg = f"Expected output file not found: {fastq_file}"
        msg += f"\nAvailable files: {list(output_prefix.parent.glob('*'))}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info(f"  Generated {fastq_file.name}")
    return fastq_file

def concatenate_files(paths: List[Path], output: Path) -> None:
    """
    Concatenate multiple files with matching extensions into a single file.
    Args:
        paths (List[Path]): List of input files
        output (Path): Output concatenated file
    """
    # First check that all files have matching extensions
    extensions = [p.suffix for p in paths]
    if len(set(extensions)) != 1:
        raise ValueError(f"Input files have different extensions: {extensions}")
    logger.info(f"Concatenating {len(paths)} files into {output.name}...")
    with open(output, 'wb') as outf:
        for path in paths:
            with open(path, 'rb') as inf:
                outf.write(inf.read())
    logger.info(f"Concatenated files into {output}.")

def fix_fastq_read_ids(input_fastq: Path, output_fastq: Path) -> None:
    """
    Fix FASTQ read IDs by converting /1 and /2 suffixes to space-delimited format.
    Converts "read_id/1" to "read_id 1" and "read_id/2" to "read_id 2".

    Args:
        input_fastq (Path): Input FASTQ file with slash-delimited mate numbers
        output_fastq (Path): Output FASTQ file with space-delimited mate numbers
    """
    logger.info(f"Fixing read IDs in {input_fastq.name}...")
    records = []
    for record in SeqIO.parse(input_fastq, "fastq"):
        # Replace /1 or /2 at the end of the ID with space-delimited format
        if record.id.endswith("/1"):
            record.id = record.id[:-2] + " 1"
            # Clear description to prevent duplication (description includes the full header line)
            record.description = ""
        elif record.id.endswith("/2"):
            record.id = record.id[:-2] + " 2"
            # Clear description to prevent duplication (description includes the full header line)
            record.description = ""

        records.append(record)

    SeqIO.write(records, output_fastq, "fastq")
    logger.info(f"Fixed {len(records)} read IDs in {output_fastq.name}")

def interleave_paired_reads(r1_file: Path, r2_file: Path, output_file: Path, file_format: str = "fastq") -> None:
    """
    Interleave paired-end reads from R1 and R2 files into a single file.

    Args:
        r1_file (Path): R1 (forward) reads file
        r2_file (Path): R2 (reverse) reads file
        output_file (Path): Output interleaved file
        file_format (str): File format, either "fastq" or "fasta"
    """
    logger.info(f"Interleaving {r1_file.name} and {r2_file.name}...")

    with open(r1_file, 'r') as f1, open(r2_file, 'r') as f2, open(output_file, 'w') as out:
        r1_records = SeqIO.parse(f1, file_format)
        r2_records = SeqIO.parse(f2, file_format)

        interleaved_records = []
        for rec1, rec2 in zip(r1_records, r2_records):
            interleaved_records.append(rec1)
            interleaved_records.append(rec2)

        SeqIO.write(interleaved_records, out, file_format)

    logger.info(f"Created interleaved file: {output_file.name}")

def fastq_to_fasta(fastq_file: Path, fasta_file: Path) -> None:
    """
    Convert FASTQ file to FASTA format.
    Args:
        fastq_file (Path): Input FASTQ file (uncompressed)
        fasta_file (Path): Output FASTA file
    """
    logger.info(f"Converting {fastq_file.name} to FASTA...")
    with open(fastq_file, 'r') as f_in:
        with open(fasta_file, 'w') as f_out:
            SeqIO.convert(f_in, "fastq", f_out, "fasta")
    logger.info(f"Converted to {fasta_file.name}.")

#############
# S3 UPLOAD #
#############

def gzip_file(local_file: Path, gzip_file: Path) -> None:
    """
    Generate a gzipped copy of a local file.
    Args:
        local_file (Path): Path to local file to gzip
        gzip_file (Path): Path to gzipped file
    """
    logger.info(f"Gzipping {local_file.name}...")
    with open(local_file, 'rb') as f_in:
        with gzip.open(gzip_file, 'wb') as f_out:
            f_out.write(f_in.read())
    logger.info(f"Gzipped {local_file.name} to {gzip_file.name}.")

def upload_to_s3(local_file: Path, s3_uri: str) -> None:
    """
    Upload a file to S3.
    Args:
        local_file (Path): Path to local file to upload
        s3_uri (str): Full S3 URI (e.g., s3://bucket/key)
    """
    # Parse S3 URI
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    parts = s3_uri.lstrip("s3://").rstrip("/").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else local_file.name
    s3_client = boto3.client('s3')
    try:
        logger.info(f"Uploading {local_file.name} to {s3_uri}...")
        s3_client.upload_file(str(local_file), bucket, key)
        logger.info(f"Uploaded {local_file.name} to {s3_uri}.")
    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        raise
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}.")
        raise

#################
# MAIN WORKFLOW #
#################

def generate_illumina_data(fasta_viral: Path,
                           fasta_other: List[Path],
                           read_pairs_per_genome: int,
                           output_prefix_local: str,
                           output_prefix_s3: str,
                           seed: int) -> None:
    """
    Generate test read data from reference genomes and upload to S3.

    Args:
        fasta_viral (Path): FASTA file for viral genome
        fasta_other (List[Path]): List of FASTA files for other genomes
        read_pairs_per_genome (int): Number of read pairs to generate per genome
        output_prefix_local (str): Local path prefix for output files
        output_prefix_s3 (str): S3 URI prefix for output files
        seed (int): Seed for random number generators
    """
    for fasta in [fasta_viral] + fasta_other:
        if not fasta.exists():
            raise FileNotFoundError(f"FASTA file not found: {fasta}")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        r1_files = []
        r2_files = []
        logger.info(f"Generating Illumina test data from each input file...")
        for fasta in [fasta_viral] + fasta_other:
            prefix = tmpdir / f"{fasta.stem}"
            r1, r2 = generate_illumina(fasta, prefix, read_pairs_per_genome, seed)
            r1_files.append(r1)
            r2_files.append(r2)
        logger.info(f"Generating extra overlapping Illumina reads from viral genomes...")
        # Create a modified copy of the viral FASTA to avoid duplicate read IDs
        viral_modified = tmpdir / "viral_overlap.fasta"
        modify_fasta_headers(fasta_viral, viral_modified, "_overlap")
        prefix = tmpdir / "viral_overlap"
        r1, r2 = generate_illumina(viral_modified, prefix, read_pairs_per_genome, seed, 200)
        r1_files.append(r1)
        r2_files.append(r2)
        logger.info(f"Concatenating all Illumina reads...")
        concat_r1_raw = tmpdir / "R1_raw.fastq"
        concat_r2_raw = tmpdir / "R2_raw.fastq"
        concatenate_files(r1_files, concat_r1_raw)
        concatenate_files(r2_files, concat_r2_raw)
        logger.info(f"Fixing read IDs to use space-delimited format...")
        concat_r1 = tmpdir / "R1.fastq"
        concat_r2 = tmpdir / "R2.fastq"
        fix_fastq_read_ids(concat_r1_raw, concat_r1)
        fix_fastq_read_ids(concat_r2_raw, concat_r2)
        logger.info(f"Generating gzipped copies of the concatenated Illumina reads...")
        gzip_r1 = concat_r1.with_suffix(".gz")
        gzip_r2 = concat_r2.with_suffix(".gz")
        gzip_file(concat_r1, gzip_r1)
        gzip_file(concat_r2, gzip_r2)
        logger.info(f"Uploading Illumina reads to S3...")
        upload_to_s3(gzip_r1, output_prefix_s3 + "R1.fastq.gz")
        upload_to_s3(gzip_r2, output_prefix_s3 + "R2.fastq.gz")
        logger.info(f"Creating unzipped local copies of the Illumina reads...")
        # Convert string prefix to Path - handle both directory paths and file prefixes
        output_prefix_path = Path(output_prefix_local)
        # Create parent directory
        if str(output_prefix_local).endswith('/'):
            # It's a directory path, create it
            output_prefix_path.mkdir(parents=True, exist_ok=True)
        else:
            # It's a file prefix, create parent directory
            output_prefix_path.parent.mkdir(parents=True, exist_ok=True)
        dest_r1 = Path(output_prefix_local + "R1.fastq")
        dest_r2 = Path(output_prefix_local + "R2.fastq")
        shutil.copy2(str(concat_r1), str(dest_r1))
        shutil.copy2(str(concat_r2), str(dest_r2))
        logger.info(f"Converting Illumina FASTQs to FASTA format...")
        fasta_r1 = Path(output_prefix_local + "R1.fasta")
        fasta_r2 = Path(output_prefix_local + "R2.fasta")
        fastq_to_fasta(dest_r1, fasta_r1)
        fastq_to_fasta(dest_r2, fasta_r2)

        logger.info(f"Creating interleaved FASTQ file...")
        interleaved_fastq = tmpdir / "interleaved.fastq"
        interleave_paired_reads(concat_r1, concat_r2, interleaved_fastq, "fastq")
        dest_interleaved_fastq = Path(output_prefix_local + "interleaved.fastq")
        shutil.copy2(str(interleaved_fastq), str(dest_interleaved_fastq))

        logger.info(f"Creating interleaved FASTA file...")
        interleaved_fasta = Path(output_prefix_local + "interleaved.fasta")
        interleave_paired_reads(fasta_r1, fasta_r2, interleaved_fasta, "fasta")

        logger.info(f"Gzipping and uploading interleaved FASTQ...")
        gzip_interleaved = interleaved_fastq.with_suffix(".fastq.gz")
        gzip_file(interleaved_fastq, gzip_interleaved)
        upload_to_s3(gzip_interleaved, output_prefix_s3 + "interleaved.fastq.gz")

        logger.info(f"Done.")

def generate_ont_data(fasta_viral: Path,
                      fasta_other: List[Path],
                      reads_per_genome: int,
                      output_prefix_local: str,
                      output_prefix_s3: str,
                      nanosim_model_url: str,
                      seed: int) -> None:
    """
    Generate ONT test read data from reference genomes and upload to S3.

    Args:
        fasta_viral (Path): FASTA file for viral genome
        fasta_other (List[Path]): List of FASTA files for other genomes
        reads_per_genome (int): Number of reads to generate per genome
        output_prefix_local (str): Local path prefix for output files
        output_prefix_s3 (str): S3 URI prefix for output files
        nanosim_model_url (str): URL to NanoSim model tarball
        seed (int): Seed for random number generators
    """
    for fasta in [fasta_viral] + fasta_other:
        if not fasta.exists():
            raise FileNotFoundError(f"FASTA file not found: {fasta}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        fastq_files = []

        # Download NanoSim model
        logger.info("Downloading NanoSim model...")
        model_dir = download_nanosim_model(tmpdir, nanosim_model_url)

        logger.info(f"Generating ONT reads from each input file...")
        # Use different seeds for each genome to ensure unique read IDs

        # Generate double reads from viral genome
        prefix = tmpdir / fasta_viral.stem
        viral_seed = seed
        fastq = generate_ont(fasta_viral, prefix, reads_per_genome * 2, model_dir, viral_seed)
        fastq_files.append(fastq)

        # Generate reads from other genomes
        for i, fasta in enumerate(fasta_other):
            prefix = tmpdir / fasta.stem
            genome_seed = seed + i + 1
            fastq = generate_ont(fasta, prefix, reads_per_genome, model_dir, genome_seed)
            fastq_files.append(fastq)

        logger.info(f"Concatenating all ONT reads...")
        concat_fastq = tmpdir / "ont.fastq"
        concatenate_files(fastq_files, concat_fastq)

        logger.info(f"Generating gzipped copy of the concatenated ONT reads...")
        gzip_fastq = concat_fastq.with_suffix(".fastq.gz")
        gzip_file(concat_fastq, gzip_fastq)

        logger.info(f"Uploading ONT reads to S3...")
        upload_to_s3(gzip_fastq, output_prefix_s3 + "ont.fastq.gz")

        logger.info(f"Creating unzipped local copy of the ONT reads...")
        # Convert string prefix to Path - handle both directory paths and file prefixes
        output_prefix_path = Path(output_prefix_local)
        # Create parent directory
        if str(output_prefix_local).endswith('/'):
            # It's a directory path, create it
            output_prefix_path.mkdir(parents=True, exist_ok=True)
        else:
            # It's a file prefix, create parent directory
            output_prefix_path.parent.mkdir(parents=True, exist_ok=True)
        dest_fastq = Path(output_prefix_local + "ont.fastq")
        shutil.copy2(str(concat_fastq), str(dest_fastq))

        logger.info(f"Converting ONT FASTQ to FASTA format...")
        fasta_file = Path(output_prefix_local + "ont.fasta")
        fastq_to_fasta(dest_fastq, fasta_file)

        logger.info(f"Done.")

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=DESC,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--fasta-viral", "-v",
        type=Path,
        default=Path("./test-data/tiny-index/genomes/hdv.fasta"),
        help="Plaintext FASTA file for viral genomes"
    )
    parser.add_argument(
        "--fasta-other", "-f",
        help="List of plaintext FASTA files for other genomes, provided as a comma-separated list of paths",
        type=lambda s: [Path(p) for p in s.split(",")],
        default=[Path(f"./test-data/tiny-index/genomes/{name}.fasta") for name in ["human", "phage", "rrna"]],
    )
    parser.add_argument(
        "--n-reads", "-n",
        type=int,
        default=5,
        help="Number of reads (Illumina pairs, ONT single reads) to generate per input file (default: 5)"
    )
    parser.add_argument(
        "--output-prefix-local", "-o",
        type=str,
        default="./test-data/tiny-index/reads/",
        help="Local path prefix for output files (default: ./test-data/tiny-index/reads/)"
    )
    parser.add_argument(
        "--output-prefix-s3", "-s",
        type=str,
        default="s3://nao-testing/tiny-test/raw/tiny-test_",
        help="S3 URI prefix for output files (default: s3://nao-testing/tiny-test/raw)"
    )
    parser.add_argument(
        "--seed", "-e",
        type=int,
        default=827100,
        help="Seed for random number generators (default: 827100)"
    )
    parser.add_argument(
        "--nanosim-model-url",
        type=str,
        default=NANOSIM_MODEL_URL,
        help=f"URL to NanoSim pre-trained model tarball (default: {NANOSIM_MODEL_URL})"
    )
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    generate_illumina_data(
        fasta_viral=args.fasta_viral,
        fasta_other=args.fasta_other,
        read_pairs_per_genome=args.n_reads,
        output_prefix_local=args.output_prefix_local,
        output_prefix_s3=args.output_prefix_s3,
        seed=args.seed
    )
    generate_ont_data(
        fasta_viral=args.fasta_viral,
        fasta_other=args.fasta_other,
        reads_per_genome=args.n_reads,
        output_prefix_local=args.output_prefix_local,
        output_prefix_s3=args.output_prefix_s3,
        nanosim_model_url=args.nanosim_model_url,
        seed=args.seed
    )

if __name__ == "__main__":
    main()
