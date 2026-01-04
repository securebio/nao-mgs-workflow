#!/usr/bin/env python3
DESC = """
Generate tiny Illumina test datasets from reference genomes using InSilicoSeq.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import logging
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
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

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
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
                   fragment_length: int | None = None,
                   model: str = "NovaSeq") -> tuple[Path, Path]:
    """
    Generate paired-end Illumina reads from a FASTA file using InSilicoSeq.
    Args:
        fasta_file (Path): Input FASTA file
        output_prefix (Path): Output prefix for generated reads
        n_read_pairs (int): Number of read pairs to generate
        seed (int): Seed for random number generators
        fragment_length (int | None): Fragment length for paired-end reads (default: None)
        model (str): Sequencing error model (default: NovaSeq)
    Returns:
        tuple[Path, Path]: Paths to R1 and R2 FASTQ files
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

def concatenate_files(paths: list[Path], output: Path) -> None:
    """
    Concatenate multiple files with matching extensions into a single file.
    Args:
        paths (list[Path]): List of input files
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
                           fasta_other: list[Path],
                           read_pairs_per_genome: int,
                           output_prefix_local: str,
                           output_prefix_s3: str,
                           seed: int) -> None:
    """
    Generate test read data from reference genomes and upload to S3.

    Args:
        fasta_viral (Path): FASTA file for viral genome
        fasta_other (list[Path]): List of FASTA files for other genomes
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
        logger.info(f"Generating test data from each input file...")
        for fasta in [fasta_viral] + fasta_other:
            prefix = tmpdir / f"{fasta.stem}"
            r1, r2 = generate_illumina(fasta, prefix, read_pairs_per_genome, seed)
            r1_files.append(r1)
            r2_files.append(r2)
        logger.info(f"Generating extra overlapping reads from viral genomes...")
        # Create a modified copy of the viral FASTA to avoid duplicate read IDs
        viral_modified = tmpdir / "viral_overlap.fasta"
        modify_fasta_headers(fasta_viral, viral_modified, "_overlap")
        prefix = tmpdir / "viral_overlap"
        r1, r2 = generate_illumina(viral_modified, prefix, read_pairs_per_genome, seed, 200)
        r1_files.append(r1)
        r2_files.append(r2)
        logger.info(f"Concatenating all reads...")
        concat_r1 = tmpdir / "R1.fastq"
        concat_r2 = tmpdir / "R2.fastq"
        concatenate_files(r1_files, concat_r1)
        concatenate_files(r2_files, concat_r2)
        logger.info(f"Generating gzipped copies of the concatenated reads...")
        gzip_r1 = concat_r1.with_suffix(".gz")
        gzip_r2 = concat_r2.with_suffix(".gz")
        gzip_file(concat_r1, gzip_r1)
        gzip_file(concat_r2, gzip_r2)
        logger.info(f"Uploading reads to S3...")
        upload_to_s3(gzip_r1, output_prefix_s3 + "R1.fastq.gz")
        upload_to_s3(gzip_r2, output_prefix_s3 + "R2.fastq.gz")
        logger.info(f"Creating unzipped local copies...")
        os.makedirs(os.path.dirname(output_prefix_local), exist_ok=True)
        dest_r1 = output_prefix_local + "R1.fastq"
        dest_r2 = output_prefix_local + "R2.fastq"
        shutil.copy2(str(concat_r1), dest_r1)
        shutil.copy2(str(concat_r2), dest_r2)
        logger.info(f"Converting FASTQs to FASTA format...")
        fasta_r1 = str(output_prefix_local) + "R1.fasta"
        fasta_r2 = str(output_prefix_local) + "R2.fasta"
        fastq_to_fasta(Path(dest_r1), Path(fasta_r1))
        fastq_to_fasta(Path(dest_r2), Path(fasta_r2))
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
        type=lambda s: s.split(","),
        default=[Path(f"./test-data/tiny-index/genomes/{name}.fasta") for name in ["human", "phage", "rrna"]],
    )
    parser.add_argument(
        "--n-read-pairs", "-n",
        type=int,
        default=5,
        help="Number of read pairs to generate per input file (default: 5)"
    )
    parser.add_argument(
        "--output-prefix-local", "-o",
        type=str,
        default="./test-data/tiny-index/reads/illumina/",
        help="Local path prefix for output files (default: ./test-data/tiny-index/reads/illumina/)"
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
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    generate_illumina_data(
        fasta_viral=args.fasta_viral,
        fasta_other=args.fasta_other,
        read_pairs_per_genome=args.n_read_pairs,
        output_prefix_local=args.output_prefix_local,
        output_prefix_s3=args.output_prefix_s3,
        seed=args.seed
    )

if __name__ == "__main__":
    main()
