use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use clap::Parser;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

// UC format column indices
const REC_TYPE: usize = 0;
const CLUSTER_ID: usize = 1;
const SIZE: usize = 2;
const PERCENT_ID: usize = 3;
const ORIENTATION: usize = 4;
const CIGAR: usize = 7;
const SEQ_ID: usize = 8;
const CLUSTER_REP_ID: usize = 9;
const UC_FIELD_COUNT: usize = 10;

/// Process tabular output from VSEARCH clustering into a unified output format
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to tabular output from VSEARCH clustering (gzipped)
    vsearch_db: String,

    /// Output path for processed data frame (gzipped TSV)
    output_db: String,

    /// Output path for representative sequence IDs for the largest clusters
    output_ids: String,

    /// Number of largest clusters to output representative sequence IDs for
    #[arg(short = 'n', long)]
    n_clusters: usize,

    /// Column name prefix for output DB (default: no prefix)
    #[arg(short = 'p', long = "output-prefix", default_value = "")]
    output_prefix: String,
}

fn open_gz_reader(path: &str) -> Result<BufReader<GzDecoder<File>>, Box<dyn Error>> {
    let file = File::open(path)?;
    Ok(BufReader::new(GzDecoder::new(file)))
}

fn open_gz_writer(path: &str) -> Result<BufWriter<GzEncoder<File>>, Box<dyn Error>> {
    let file = File::create(path)?;
    Ok(BufWriter::new(GzEncoder::new(file, Compression::default())))
}

fn parse_field<T: std::str::FromStr>(
    fields: &[&str],
    index: usize,
    line_num: usize,
    field_name: &str,
) -> Result<T, Box<dyn Error>>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    fields[index].parse::<T>().map_err(|e| {
        format!("Line {}: invalid {} '{}': {}", line_num, field_name, fields[index], e).into()
    })
}

fn format_header(prefix: &str) -> String {
    if prefix.is_empty() {
        "seq_id\tcluster_id\tcluster_rep_id\tseq_length\tis_cluster_rep\tpercent_identity\torientation\tcigar\tcluster_size".to_string()
    } else {
        format!(
            "seq_id\t{p}_cluster_id\t{p}_cluster_rep_id\t{p}_seq_length\t{p}_is_cluster_rep\t{p}_percent_identity\t{p}_orientation\t{p}_cigar\t{p}_cluster_size",
            p = prefix
        )
    }
}

/// Pass 1: Build cluster_sizes and cluster_reps lookup tables
fn build_lookup_tables(
    input_path: &str,
) -> Result<(HashMap<u64, u64>, HashMap<u64, String>), Box<dyn Error>> {
    eprintln!("Pass 1: Building lookup tables...");

    let reader = open_gz_reader(input_path)?;
    let mut cluster_sizes: HashMap<u64, u64> = HashMap::new();
    let mut cluster_reps: HashMap<u64, String> = HashMap::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line_num = line_num + 1;
        let line = line_result?;
        let fields: Vec<&str> = line.split('\t').collect();

        if fields.len() != UC_FIELD_COUNT {
            return Err(format!(
                "Line {}: expected {} fields, found {}", line_num, UC_FIELD_COUNT, fields.len()
            ).into());
        }

        match fields[REC_TYPE] {
            "C" => {
                let cluster_id: u64 = parse_field(&fields, CLUSTER_ID, line_num, "cluster_id")?;
                let cluster_size: u64 = parse_field(&fields, SIZE, line_num, "cluster_size")?;
                cluster_sizes.insert(cluster_id, cluster_size);
            }
            "S" => {
                let cluster_id: u64 = parse_field(&fields, CLUSTER_ID, line_num, "cluster_id")?;
                cluster_reps.insert(cluster_id, fields[SEQ_ID].to_string());
            }
            "H" => {}
            other => return Err(format!("Line {}: unknown record type '{}'", line_num, other).into()),
        }
    }

    eprintln!("Pass 1 complete: {} clusters, {} representatives", cluster_sizes.len(), cluster_reps.len());
    Ok((cluster_sizes, cluster_reps))
}

/// Pass 2: Stream through file and write TSV output
fn write_tsv_output(
    input_path: &str,
    output_path: &str,
    prefix: &str,
    cluster_sizes: &HashMap<u64, u64>,
) -> Result<(), Box<dyn Error>> {
    eprintln!("Pass 2: Writing TSV output...");

    let reader = open_gz_reader(input_path)?;
    let mut writer = open_gz_writer(output_path)?;
    writeln!(writer, "{}", format_header(prefix))?;

    let mut records_written = 0;

    for (line_num, line_result) in reader.lines().enumerate() {
        let line_num = line_num + 1;
        let line = line_result?;
        let fields: Vec<&str> = line.split('\t').collect();

        if fields.len() != UC_FIELD_COUNT {
            return Err(format!(
                "Line {}: expected {} fields, found {}", line_num, UC_FIELD_COUNT, fields.len()
            ).into());
        }

        match fields[REC_TYPE] {
            "H" => {
                let cluster_id: u64 = parse_field(&fields, CLUSTER_ID, line_num, "cluster_id")?;
                let cluster_size = cluster_sizes.get(&cluster_id).ok_or_else(|| {
                    format!("Line {}: cluster_id {} not found in lookup table", line_num, cluster_id)
                })?;
                writeln!(writer, "{}\t{}\t{}\t{}\tFalse\t{}\t{}\t{}\t{}",
                    fields[SEQ_ID], cluster_id, fields[CLUSTER_REP_ID], fields[SIZE],
                    fields[PERCENT_ID], fields[ORIENTATION], fields[CIGAR], cluster_size)?;
                records_written += 1;
            }
            "S" => {
                let cluster_id: u64 = parse_field(&fields, CLUSTER_ID, line_num, "cluster_id")?;
                let seq_length: u64 = parse_field(&fields, SIZE, line_num, "seq_length")?;
                let cluster_size = cluster_sizes.get(&cluster_id).ok_or_else(|| {
                    format!("Line {}: cluster_id {} not found in lookup table", line_num, cluster_id)
                })?;
                writeln!(writer, "{}\t{}\t{}\t{}\tTrue\t100.0\t+\t{}M\t{}",
                    fields[SEQ_ID], cluster_id, fields[SEQ_ID], seq_length, seq_length, cluster_size)?;
                records_written += 1;
            }
            "C" => {}
            other => return Err(format!("Line {}: unknown record type '{}'", line_num, other).into()),
        }
    }

    writer.flush()?;
    eprintln!("Pass 2 complete: {} records written", records_written);
    Ok(())
}

/// Extract top N representative IDs by cluster size
fn write_top_representatives(
    output_path: &str,
    n_clusters: usize,
    cluster_sizes: &HashMap<u64, u64>,
    cluster_reps: &HashMap<u64, String>,
) -> Result<(), Box<dyn Error>> {
    eprintln!("Step 3: Extracting top {} representative IDs...", n_clusters);

    let mut clusters: Vec<(u64, &String)> = cluster_reps
        .iter()
        .filter_map(|(cluster_id, rep_id)| cluster_sizes.get(cluster_id).map(|&size| (size, rep_id)))
        .collect();

    // Sort by cluster_size descending, then representative_id ascending (tie-breaker)
    clusters.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));

    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);
    let n = std::cmp::min(n_clusters, clusters.len());
    for (_, rep_id) in clusters.iter().take(n) {
        writeln!(writer, "{}", rep_id)?;
    }

    writer.flush()?;
    eprintln!("Step 3 complete: {} representative IDs written", n);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    eprintln!("Processing VSEARCH cluster output");
    eprintln!("  Input: {}", args.vsearch_db);
    eprintln!("  Output TSV: {}", args.output_db);
    eprintln!("  Output IDs: {}", args.output_ids);
    eprintln!("  N clusters: {}", args.n_clusters);
    eprintln!("  Prefix: {}", if args.output_prefix.is_empty() { "(none)" } else { &args.output_prefix });

    let (cluster_sizes, cluster_reps) = build_lookup_tables(&args.vsearch_db)?;
    write_tsv_output(&args.vsearch_db, &args.output_db, &args.output_prefix, &cluster_sizes)?;
    write_top_representatives(&args.output_ids, args.n_clusters, &cluster_sizes, &cluster_reps)?;

    eprintln!("Done.");
    Ok(())
}
