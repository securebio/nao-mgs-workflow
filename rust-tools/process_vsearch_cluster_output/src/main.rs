// ============================================================================
// IMPORTS
// ============================================================================

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use clap::Parser;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

// ============================================================================
// UC FORMAT COLUMN INDICES
// ============================================================================

const REC_TYPE: usize = 0;
const CLUSTER_ID: usize = 1;
const SIZE: usize = 2;
const PERCENT_ID: usize = 3;
const ORIENTATION: usize = 4;
const CIGAR: usize = 7;
const SEQ_ID: usize = 8;
const CLUSTER_REP_ID: usize = 9;

const UC_FIELD_COUNT: usize = 10;

// ============================================================================
// ARGUMENT PARSING
// ============================================================================

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

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Open a gzipped file for reading
fn open_gz_reader(path: &str) -> Result<BufReader<GzDecoder<File>>, Box<dyn Error>> {
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    Ok(BufReader::new(decoder))
}

/// Open a gzipped file for writing
fn open_gz_writer(path: &str) -> Result<BufWriter<GzEncoder<File>>, Box<dyn Error>> {
    let file = File::create(path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    Ok(BufWriter::new(encoder))
}

/// Format the TSV header with optional prefix
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

// ============================================================================
// PASS 1: BUILD LOOKUP TABLES
// ============================================================================

/// Pass 1: Build cluster_sizes and cluster_reps lookup tables
fn build_lookup_tables(
    input_path: &str,
) -> Result<(HashMap<u64, u64>, HashMap<u64, String>), Box<dyn Error>> {
    eprintln!("Pass 1: Building lookup tables...");

    let reader = open_gz_reader(input_path)?;
    let mut cluster_sizes: HashMap<u64, u64> = HashMap::new();
    let mut cluster_reps: HashMap<u64, String> = HashMap::new();

    let mut line_num = 0;
    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result?;
        let fields: Vec<&str> = line.split('\t').collect();

        if fields.len() != UC_FIELD_COUNT {
            return Err(format!(
                "Line {}: expected {} fields, found {}",
                line_num,
                UC_FIELD_COUNT,
                fields.len()
            )
            .into());
        }

        let record_type = fields[REC_TYPE];

        match record_type {
            "C" => {
                // Cluster summary record: extract cluster_id and cluster_size
                let cluster_id: u64 = fields[CLUSTER_ID].parse().map_err(|e| {
                    format!("Line {}: invalid cluster_id '{}': {}", line_num, fields[CLUSTER_ID], e)
                })?;
                let cluster_size: u64 = fields[SIZE].parse().map_err(|e| {
                    format!("Line {}: invalid cluster_size '{}': {}", line_num, fields[SIZE], e)
                })?;
                cluster_sizes.insert(cluster_id, cluster_size);
            }
            "S" => {
                // Seed (representative) record: extract cluster_id and representative seq_id
                let cluster_id: u64 = fields[CLUSTER_ID].parse().map_err(|e| {
                    format!("Line {}: invalid cluster_id '{}': {}", line_num, fields[CLUSTER_ID], e)
                })?;
                let representative_id = fields[SEQ_ID].to_string();
                cluster_reps.insert(cluster_id, representative_id);
            }
            "H" => {
                // Hit record: skip in pass 1
            }
            _ => {
                return Err(format!(
                    "Line {}: unknown record type '{}'",
                    line_num, record_type
                )
                .into());
            }
        }
    }

    eprintln!(
        "Pass 1 complete: {} clusters, {} representatives",
        cluster_sizes.len(),
        cluster_reps.len()
    );

    Ok((cluster_sizes, cluster_reps))
}

// ============================================================================
// PASS 2: STREAM TSV OUTPUT
// ============================================================================

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

    // Write header
    writeln!(writer, "{}", format_header(prefix))?;

    let mut line_num = 0;
    let mut records_written = 0;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result?;
        let fields: Vec<&str> = line.split('\t').collect();

        if fields.len() != UC_FIELD_COUNT {
            return Err(format!(
                "Line {}: expected {} fields, found {}",
                line_num,
                UC_FIELD_COUNT,
                fields.len()
            )
            .into());
        }

        let record_type = fields[REC_TYPE];

        match record_type {
            "H" => {
                // Hit record: output sequence info
                let seq_id = fields[SEQ_ID];
                let cluster_id: u64 = fields[CLUSTER_ID].parse().map_err(|e| {
                    format!("Line {}: invalid cluster_id '{}': {}", line_num, fields[CLUSTER_ID], e)
                })?;
                let cluster_rep_id = fields[CLUSTER_REP_ID];
                let seq_length = fields[SIZE];
                let percent_identity = fields[PERCENT_ID];
                let orientation = fields[ORIENTATION];
                let cigar = fields[CIGAR];
                let is_cluster_rep = "False";

                let cluster_size = cluster_sizes.get(&cluster_id).ok_or_else(|| {
                    format!("Line {}: cluster_id {} not found in lookup table", line_num, cluster_id)
                })?;

                writeln!(
                    writer,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    seq_id,
                    cluster_id,
                    cluster_rep_id,
                    seq_length,
                    is_cluster_rep,
                    percent_identity,
                    orientation,
                    cigar,
                    cluster_size
                )?;
                records_written += 1;
            }
            "S" => {
                // Seed (representative) record: output with synthetic values
                let seq_id = fields[SEQ_ID];
                let cluster_id: u64 = fields[CLUSTER_ID].parse().map_err(|e| {
                    format!("Line {}: invalid cluster_id '{}': {}", line_num, fields[CLUSTER_ID], e)
                })?;
                let cluster_rep_id = seq_id; // Representative is itself
                let seq_length: u64 = fields[SIZE].parse().map_err(|e| {
                    format!("Line {}: invalid seq_length '{}': {}", line_num, fields[SIZE], e)
                })?;
                let percent_identity = "100.0"; // Implicit: perfect self-match
                let orientation = "+"; // Implicit: same strand
                let cigar = format!("{}M", seq_length); // Implicit: full match
                let is_cluster_rep = "True";

                let cluster_size = cluster_sizes.get(&cluster_id).ok_or_else(|| {
                    format!("Line {}: cluster_id {} not found in lookup table", line_num, cluster_id)
                })?;

                writeln!(
                    writer,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    seq_id,
                    cluster_id,
                    cluster_rep_id,
                    seq_length,
                    is_cluster_rep,
                    percent_identity,
                    orientation,
                    cigar,
                    cluster_size
                )?;
                records_written += 1;
            }
            "C" => {
                // Cluster summary record: skip in pass 2
            }
            _ => {
                return Err(format!(
                    "Line {}: unknown record type '{}'",
                    line_num, record_type
                )
                .into());
            }
        }
    }

    writer.flush()?;
    eprintln!("Pass 2 complete: {} records written", records_written);

    Ok(())
}

// ============================================================================
// STEP 3: EXTRACT TOP N REPRESENTATIVE IDS
// ============================================================================

/// Extract top N representative IDs by cluster size
fn write_top_representatives(
    output_path: &str,
    n_clusters: usize,
    cluster_sizes: &HashMap<u64, u64>,
    cluster_reps: &HashMap<u64, String>,
) -> Result<(), Box<dyn Error>> {
    eprintln!("Step 3: Extracting top {} representative IDs...", n_clusters);

    // Build sortable list of (cluster_size, representative_id) pairs
    let mut clusters: Vec<(u64, &String)> = cluster_reps
        .iter()
        .filter_map(|(cluster_id, rep_id)| {
            cluster_sizes.get(cluster_id).map(|&size| (size, rep_id))
        })
        .collect();

    // Sort by cluster_size descending, then representative_id ascending (tie-breaker)
    clusters.sort_by(|a, b| {
        match b.0.cmp(&a.0) {
            std::cmp::Ordering::Equal => a.1.cmp(b.1),
            other => other,
        }
    });

    // Write top N to output file (plain text, not gzipped)
    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    let n = std::cmp::min(n_clusters, clusters.len());
    for i in 0..n {
        writeln!(writer, "{}", clusters[i].1)?;
    }

    writer.flush()?;
    eprintln!("Step 3 complete: {} representative IDs written", n);

    Ok(())
}

// ============================================================================
// MAIN
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    eprintln!("Processing VSEARCH cluster output");
    eprintln!("  Input: {}", args.vsearch_db);
    eprintln!("  Output TSV: {}", args.output_db);
    eprintln!("  Output IDs: {}", args.output_ids);
    eprintln!("  N clusters: {}", args.n_clusters);
    eprintln!("  Prefix: {}", if args.output_prefix.is_empty() { "(none)" } else { &args.output_prefix });

    // Pass 1: Build lookup tables
    let (cluster_sizes, cluster_reps) = build_lookup_tables(&args.vsearch_db)?;

    // Pass 2: Write TSV output
    write_tsv_output(
        &args.vsearch_db,
        &args.output_db,
        &args.output_prefix,
        &cluster_sizes,
    )?;

    // Step 3: Write top N representative IDs
    write_top_representatives(
        &args.output_ids,
        args.n_clusters,
        &cluster_sizes,
        &cluster_reps,
    )?;

    eprintln!("Done.");
    Ok(())
}

// ============================================================================
// UNIT TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // format_header tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_format_header_no_prefix() {
        let header = format_header("");
        assert_eq!(
            header,
            "seq_id\tcluster_id\tcluster_rep_id\tseq_length\tis_cluster_rep\tpercent_identity\torientation\tcigar\tcluster_size"
        );
    }

    #[test]
    fn test_format_header_with_prefix() {
        let header = format_header("vsearch");
        assert_eq!(
            header,
            "seq_id\tvsearch_cluster_id\tvsearch_cluster_rep_id\tvsearch_seq_length\tvsearch_is_cluster_rep\tvsearch_percent_identity\tvsearch_orientation\tvsearch_cigar\tvsearch_cluster_size"
        );
    }

    #[test]
    fn test_format_header_column_count() {
        let header = format_header("test");
        let columns: Vec<&str> = header.split('\t').collect();
        assert_eq!(columns.len(), 9);
        assert_eq!(columns[0], "seq_id"); // seq_id never gets prefix
    }

    // -------------------------------------------------------------------------
    // UC line parsing tests
    // -------------------------------------------------------------------------

    fn parse_uc_line(line: &str) -> Result<Vec<&str>, String> {
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() != UC_FIELD_COUNT {
            return Err(format!("expected {} fields, found {}", UC_FIELD_COUNT, fields.len()));
        }
        Ok(fields)
    }

    #[test]
    fn test_parse_seed_record() {
        let line = "S\t0\t297\t*\t*\t*\t*\t*\tseq_001\t*";
        let fields = parse_uc_line(line).unwrap();

        assert_eq!(fields[REC_TYPE], "S");
        assert_eq!(fields[CLUSTER_ID], "0");
        assert_eq!(fields[SIZE], "297");
        assert_eq!(fields[SEQ_ID], "seq_001");
    }

    #[test]
    fn test_parse_hit_record() {
        let line = "H\t0\t297\t99.5\t+\t0\t0\t297M\tseq_002\tseq_001";
        let fields = parse_uc_line(line).unwrap();

        assert_eq!(fields[REC_TYPE], "H");
        assert_eq!(fields[CLUSTER_ID], "0");
        assert_eq!(fields[SIZE], "297");
        assert_eq!(fields[PERCENT_ID], "99.5");
        assert_eq!(fields[ORIENTATION], "+");
        assert_eq!(fields[CIGAR], "297M");
        assert_eq!(fields[SEQ_ID], "seq_002");
        assert_eq!(fields[CLUSTER_REP_ID], "seq_001");
    }

    #[test]
    fn test_parse_cluster_record() {
        let line = "C\t0\t5\t*\t*\t*\t*\t*\tseq_001\t*";
        let fields = parse_uc_line(line).unwrap();

        assert_eq!(fields[REC_TYPE], "C");
        assert_eq!(fields[CLUSTER_ID], "0");
        assert_eq!(fields[SIZE], "5"); // cluster_size for C records
    }

    #[test]
    fn test_parse_wrong_field_count() {
        let line = "S\t0\t297\t*\t*"; // Only 5 fields
        let result = parse_uc_line(line);
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // Top N sorting tests
    // -------------------------------------------------------------------------

    fn sort_clusters(clusters: &mut Vec<(u64, String)>) {
        clusters.sort_by(|a, b| {
            match b.0.cmp(&a.0) {
                std::cmp::Ordering::Equal => a.1.cmp(&b.1),
                other => other,
            }
        });
    }

    #[test]
    fn test_sort_by_size_descending() {
        let mut clusters = vec![
            (5, "seq_a".to_string()),
            (10, "seq_b".to_string()),
            (3, "seq_c".to_string()),
        ];
        sort_clusters(&mut clusters);

        assert_eq!(clusters[0].0, 10); // Largest first
        assert_eq!(clusters[1].0, 5);
        assert_eq!(clusters[2].0, 3);
    }

    #[test]
    fn test_sort_tiebreaker_by_id_ascending() {
        let mut clusters = vec![
            (5, "seq_zebra".to_string()),
            (5, "seq_apple".to_string()),
            (5, "seq_mango".to_string()),
        ];
        sort_clusters(&mut clusters);

        // Same size, should be sorted by ID ascending
        assert_eq!(clusters[0].1, "seq_apple");
        assert_eq!(clusters[1].1, "seq_mango");
        assert_eq!(clusters[2].1, "seq_zebra");
    }

    #[test]
    fn test_sort_mixed() {
        let mut clusters = vec![
            (10, "seq_b".to_string()),
            (5, "seq_z".to_string()),
            (10, "seq_a".to_string()),
            (5, "seq_a".to_string()),
        ];
        sort_clusters(&mut clusters);

        // Size 10 first (sorted by ID), then size 5 (sorted by ID)
        assert_eq!(clusters[0], (10, "seq_a".to_string()));
        assert_eq!(clusters[1], (10, "seq_b".to_string()));
        assert_eq!(clusters[2], (5, "seq_a".to_string()));
        assert_eq!(clusters[3], (5, "seq_z".to_string()));
    }
}
