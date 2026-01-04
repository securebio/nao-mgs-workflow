#!/usr/bin/env python3
DESC = """
Build tiny test databases (Kraken2 and taxonomy) from minimal reference sequences.
"""

###########
# IMPORTS #
###########

import argparse
import gzip
import bz2
import logging
import re
import subprocess
import sys
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3

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

##########################
# KRAKEN2 DATABASE SETUP #
##########################

def parse_config(config_path: Path) -> dict[str, str]:
    """
    Extract URLs from Groovy config file.
    Args:
        config_path (Path): Path to Nextflow config file
    Returns:
        dict[str, str]: Dictionary mapping sequence names to URLs
    """
    urls = {}
    with open(config_path) as f:
        content = f.read()
    # Extract human_url
    match = re.search(r'human_url\s*=\s*"([^"]+)"', content)
    if match:
        urls['human'] = match.group(1)
    # Extract phage URL from genome_urls map
    match = re.search(r'phage:\s*"([^"]+)"', content)
    if match:
        urls['phage'] = match.group(1)
    # Extract SSU URL
    match = re.search(r'ssu_url\s*=\s*"([^"]+)"', content)
    if match:
        urls['ssu'] = match.group(1)
    return urls

def setup_kraken_taxonomy(output_dir: Path, taxonomy_nodes: Path, taxonomy_names: Path) -> None:
    """
    Copy minimal taxonomy files to Kraken database directory.
    Args:
        output_dir (Path): Kraken2 database output directory
        taxonomy_nodes (Path): Path to tiny taxonomy nodes.dmp file
        taxonomy_names (Path): Path to tiny taxonomy names.dmp file
    """
    taxonomy_dir = output_dir / "taxonomy"
    taxonomy_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Setting up minimal taxonomy files...")
    shutil.copy(taxonomy_nodes, taxonomy_dir / "nodes.dmp")
    shutil.copy(taxonomy_names, taxonomy_dir / "names.dmp")

def add_kraken_taxid_tags(fasta_content: str, taxid: int) -> str:
    """
    Add Kraken taxid tags to FASTA headers.
    Args:
        fasta_content (str): FASTA format string
        taxid (int): NCBI taxonomy ID to tag sequences with
    Returns:
        str: FASTA content with kraken:taxid tags added
    """
    return re.sub(
        r'^>',
        f'>kraken:taxid|{taxid}|',
        fasta_content,
        flags=re.MULTILINE
    )

def download_sequence(url: str) -> str:
    """
    Download sequence from URL.
    Args:
        url (str): URL to download FASTA from
    Returns:
        str: FASTA content as string
    """
    with urlopen(url) as response:
        return response.read().decode('utf-8')

def open_by_suffix(filename: str | Path, mode: str = "r"):
    """
    Parse the suffix of a filename to determine the right open method
    to use, then open the file. Can handle .gz, .bz2, and uncompressed files.
    Args:
        filename (str | Path): Path to file to open
        mode (str): File open mode (default "r")
    Returns:
        File handle appropriate for the file compression type
    """
    filename_str = str(filename)
    if filename_str.endswith(".gz"):
        return gzip.open(filename_str, mode + "t")
    elif filename_str.endswith(".bz2"):
        return bz2.BZ2File(filename_str, mode)
    else:
        return open(filename_str, mode)

def add_sequence_to_kraken_library(fasta_path: Path, db_path: Path) -> None:
    """
    Add a FASTA file to Kraken2 library using kraken2-build.
    Args:
        fasta_path (Path): Path to FASTA file to add
        db_path (Path): Path to Kraken2 database directory
    """
    result = subprocess.run(
        ["kraken2-build", "--add-to-library", str(fasta_path), "--db", str(db_path)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"Failed to add {fasta_path.name} to library")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)

def add_sequences_to_kraken_library(
    sequences: list[tuple[str, str | Path, int]],
    output_dir: Path
) -> None:
    """
    Acquire sequences, tag them, and add to Kraken2 library.
    Args:
        sequences (list[tuple[str, str | Path, int]]): List of (filename, source, taxid) tuples where source is URL or Path
        output_dir (Path): Kraken2 database directory
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        for filename, source, taxid in sequences:
            output_path = tmpdir / filename
            if isinstance(source, Path):
                with open_by_suffix(source) as f:
                    content = f.read()
            else:
                content = download_sequence(source)
            tagged_content = add_kraken_taxid_tags(content, taxid)
            output_path.write_text(tagged_content)
            add_sequence_to_kraken_library(output_path, output_dir)

def build_kraken_database(
    output_dir: Path,
    sequences: list[tuple[str, str | Path, int]]
) -> None:
    """
    Build tiny Kraken2 database using provided sequences.
    Args:
        output_dir (Path): Kraken2 database output directory
        sequences (list[tuple[str, str | Path, int]]): List of (filename, source, taxid) tuples where source is URL or Path
    """
    library_dir = output_dir / "library" / "added"
    library_dir.mkdir(parents=True, exist_ok=True)
    add_sequences_to_kraken_library(sequences, output_dir)
    result = subprocess.run([
        "kraken2-build", "--build", "--db", str(output_dir),
        "--threads", "4",
        "--kmer-len", "25",
        "--minimizer-len", "15",
        "--minimizer-spaces", "3"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("Failed to build Kraken2 database")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)

    # Build Bracken k-mer distribution files
    logger.info("Building Bracken k-mer distribution files...")
    for read_len in [100, 150]:  # Common Illumina read lengths
        result = subprocess.run([
            "bracken-build",
            "-d", str(output_dir),
            "-t", "4",
            "-k", "25",
            "-l", str(read_len)
        ], capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"Failed to build Bracken distribution for read length {read_len}")
            logger.warning(result.stderr)

    subprocess.run(
        ["kraken2-build", "--clean", "--db", str(output_dir)],
        check=True,
        capture_output=True
    )

##################
# BLAST DATABASE #
##################

def build_blast_database(output_dir: Path, sequences: list[tuple[str, str | Path, int]]) -> None:
    """
    Build tiny BLAST database using provided sequences.
    Args:
        output_dir (Path): BLAST database output directory
        sequences (list[tuple[str, str | Path, int]]): List of (filename, source, taxid) tuples
    """
    # Collect all sequences into a single FASTA file and track seq_id -> taxid mapping
    combined_fasta = output_dir / "sequences.fasta"
    taxid_map_file = output_dir / "taxid_map.txt"

    seq_id_to_taxid = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        with open(combined_fasta, 'w') as outfile:
            for filename, source, taxid in sequences:
                # Download or load the sequence
                if isinstance(source, Path):
                    with open_by_suffix(source) as f:
                        content = f.read()
                else:
                    content = download_sequence(source)

                # Process FASTA content to simplify headers for BLAST
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if line.startswith('>'):
                        # Extract simple sequence ID (e.g., "NC_000021.9" from ">NC_000021.9 ...")
                        # or "AB065370.1" from ">ENA|AB065370|AB065370.1 ..."
                        parts = line[1:].split()
                        if parts:
                            # Handle different formats
                            seq_id = parts[0]
                            if '|' in seq_id:
                                # For formats like "ENA|AB065370|AB065370.1", take the last part
                                seq_id = seq_id.split('|')[-1]
                            seq_id_to_taxid[seq_id] = taxid
                            # Write simplified header
                            lines[i] = f'>{seq_id}'

                # Write modified content to combined file
                outfile.write('\n'.join(lines))
                if not content.endswith('\n'):
                    outfile.write('\n')

    # Write taxid mapping file for BLAST
    with open(taxid_map_file, 'w') as mapfile:
        for seq_id, taxid in seq_id_to_taxid.items():
            mapfile.write(f"{seq_id}\t{taxid}\n")

    # Build BLAST database with taxonomy information
    logger.info("Building BLAST database...")
    result = subprocess.run([
        "makeblastdb",
        "-in", str(combined_fasta),
        "-dbtype", "nucl",
        "-out", str(output_dir / "tiny_blast_db"),
        "-title", "Tiny BLAST Database",
        "-parse_seqids",
        "-taxid_map", str(taxid_map_file)
    ], capture_output=True, text=True)

    if result.returncode != 0:
        logger.error("Failed to build BLAST database")
        logger.error(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)

####################
# ARCHIVE CREATION #
####################

def create_archives(kraken_dir: Path,
                    blast_dir: Path,
                    taxonomy_nodes: Path,
                    taxonomy_names: Path) -> tuple[Path, Path, Path]:
    """
    Create tarball and zip archives for distribution.
    Args:
        kraken_dir (Path): Kraken2 database directory
        blast_dir (Path): BLAST database directory
        taxonomy_nodes (Path): Path to tiny-taxonomy-nodes.dmp
        taxonomy_names (Path): Path to tiny-taxonomy-names.dmp
    Returns:
        tuple[Path, Path, Path]: Tuple of (kraken_tarball_path, blast_tarball_path, taxonomy_zip_path)
    """
    # Create Kraken tarball with files at root level (no wrapper directory)
    kraken_tarball = kraken_dir.with_suffix('.tar.gz')
    subprocess.run([
        "tar", "-czf", str(kraken_tarball),
        "-C", str(kraken_dir),
        "."
    ], check=True)

    # Create BLAST tarball with directory wrapper (BLAST expects -db path/to/db_prefix)
    blast_tarball = blast_dir.with_suffix('.tar.gz')
    subprocess.run([
        "tar", "-czf", str(blast_tarball),
        "-C", str(blast_dir.parent),
        blast_dir.name
    ], check=True)

    taxonomy_zip = kraken_dir.parent / "tiny-taxonomy.zip"
    subprocess.run([
        "zip", "-q", "-j", str(taxonomy_zip),
        str(taxonomy_nodes),
        str(taxonomy_names)
    ], check=True)
    return kraken_tarball, blast_tarball, taxonomy_zip

################
# S3 UPLOAD    #
################

def upload_to_s3(local_file: Path, bucket: str, key: str) -> str:
    """
    Upload a file to S3.
    Args:
        local_file (Path): Path to local file to upload
        bucket (str): S3 bucket name
        key (str): S3 object key
    Returns:
        str: HTTPS URL for the uploaded file
    """
    s3_client = boto3.client("s3")
    logger.info(f"Uploading {local_file.name} to s3://{bucket}/{key}...")
    s3_client.upload_file(str(local_file), bucket, key)
    https_url = f"https://{bucket}.s3.amazonaws.com/{key}"
    logger.info(f"  Uploaded to {https_url}")
    return https_url

########
# MAIN #
########

def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description=DESC)
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    default_parent_dir = repo_root / "test-data" / "tiny-index"
    default_genomes_dir = default_parent_dir / "genomes"
    default_taxonomy_dir = default_parent_dir / "taxonomy"
    parser.add_argument(
        "--viral-genome",
        type=Path,
        help=f"Path to HDV genome FASTA file (default: {default_genomes_dir / 'hdv.fasta'})",
        default=default_genomes_dir / "hdv.fasta"
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=repo_root / "configs" / "index-for-run-test.config",
        help="Path to config file containing reference URLs (default: configs/index-for-run-test.config)"
    )
    parser.add_argument(
        "--taxonomy-nodes",
        type=Path,
        default=default_taxonomy_dir / "nodes.dmp",
        help=f"Path to taxonomy nodes.dmp file (default: {default_taxonomy_dir / 'nodes.dmp'})"
    )
    parser.add_argument(
        "--taxonomy-names",
        type=Path,
        default=default_taxonomy_dir / "names.dmp",
        help=f"Path to taxonomy names.dmp file (default: {default_taxonomy_dir / 'names.dmp'})"
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default="nao-testing",
        help="S3 bucket for upload (default: nao-testing)"
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="test-databases",
        help="S3 key prefix for uploaded files (default: test-databases)"
    )
    return parser.parse_args()


def validate_inputs(args: argparse.Namespace) -> None:
    """
    Validate that all required input files exist.
    Args:
        args (argparse.Namespace): Parsed command-line arguments
    """
    required_files = {
        "Config file": args.config_file,
        "Viral genome file": args.viral_genome,
        "Taxonomy nodes file": args.taxonomy_nodes,
        "Taxonomy names file": args.taxonomy_names,
    }
    for file_desc, file_path in required_files.items():
        if not file_path.exists():
            raise FileNotFoundError(f"{file_desc} not found: {file_path}")

def run_build(args: argparse.Namespace) -> None:
    """
    Execute the database build process.
    Args:
        args (argparse.Namespace): Parsed command-line arguments
    """
    urls = parse_config(args.config_file)

    # Build in temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        kraken_dir = tmpdir / "tiny-kraken2-db"
        blast_dir = tmpdir / "tiny_blast_db"
        kraken_dir.mkdir(parents=True, exist_ok=True)
        blast_dir.mkdir(parents=True, exist_ok=True)

        sequences = [
            ("human_chr21.fna", urls['human'], 9606),
            ("t4_phage.fna", urls['phage'], 10665),
            ("bsubtilis_rrna.fna", urls['ssu'], 1423),
            ("hdv.fna", args.viral_genome, 12475),
        ]

        logger.info("Step 1: Setting up minimal taxonomy...")
        setup_kraken_taxonomy(kraken_dir, args.taxonomy_nodes, args.taxonomy_names)
        logger.info("Step 2: Building Kraken2 database...")
        build_kraken_database(kraken_dir, sequences)
        logger.info("Step 3: Building BLAST database...")
        build_blast_database(blast_dir, sequences)
        logger.info("Step 4: Creating distribution archives...")
        kraken_tarball, blast_tarball, taxonomy_zip = create_archives(
            kraken_dir, blast_dir, args.taxonomy_nodes, args.taxonomy_names
        )
        logger.info("Step 5: Uploading to S3...")
        kraken_url = upload_to_s3(
            kraken_tarball,
            args.s3_bucket,
            f"{args.s3_prefix}/{kraken_tarball.name}"
        )
        blast_url = upload_to_s3(
            blast_tarball,
            args.s3_bucket,
            f"{args.s3_prefix}/{blast_tarball.name}"
        )
        taxonomy_url = upload_to_s3(
            taxonomy_zip,
            args.s3_bucket,
            f"{args.s3_prefix}/{taxonomy_zip.name}"
        )

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    validate_inputs(args)
    run_build(args)

if __name__ == "__main__":
    main()
