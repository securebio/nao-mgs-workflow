//! Integration tests for process_vsearch_cluster_output
//!
//! These tests run the compiled binary on small fixture files and verify outputs.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::Command;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

/// Get path to the compiled binary
fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_process_vsearch_cluster_output"))
}

/// Get path to test fixtures directory
fn fixtures_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("fixtures");
    path
}

/// Create a gzipped version of a file
fn gzip_file(input_path: &PathBuf, output_path: &PathBuf) {
    let input = fs::read_to_string(input_path).expect("Failed to read input file");
    let file = File::create(output_path).expect("Failed to create output file");
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder
        .write_all(input.as_bytes())
        .expect("Failed to write gzipped data");
    encoder.finish().expect("Failed to finish gzip encoding");
}

/// Read a gzipped file and return lines
fn read_gzipped_lines(path: &PathBuf) -> Vec<String> {
    let file = File::open(path).expect("Failed to open gzipped file");
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);
    reader.lines().map(|l| l.expect("Failed to read line")).collect()
}

/// Read a plain text file and return lines
fn read_lines(path: &PathBuf) -> Vec<String> {
    let file = File::open(path).expect("Failed to open file");
    let reader = BufReader::new(file);
    reader.lines().map(|l| l.expect("Failed to read line")).collect()
}

// =============================================================================
// Integration Tests
// =============================================================================

#[test]
fn test_end_to_end_basic() {
    let fixtures = fixtures_dir();
    let input_uc = fixtures.join("tiny.uc");
    let input_gz = fixtures.join("tiny.uc.gz");
    let output_tsv = fixtures.join("output.tsv.gz");
    let output_ids = fixtures.join("output_ids.txt");

    // Create gzipped input
    gzip_file(&input_uc, &input_gz);

    // Run the binary
    let output = Command::new(binary_path())
        .args([
            input_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "10",
        ])
        .output()
        .expect("Failed to execute binary");

    // Check it succeeded
    assert!(
        output.status.success(),
        "Binary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify outputs exist
    assert!(output_tsv.exists(), "TSV output not created");
    assert!(output_ids.exists(), "IDs output not created");

    // Cleanup
    fs::remove_file(&input_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}

#[test]
fn test_output_tsv_structure() {
    let fixtures = fixtures_dir();
    let input_uc = fixtures.join("tiny.uc");
    let input_gz = fixtures.join("tiny_struct.uc.gz");
    let output_tsv = fixtures.join("struct_output.tsv.gz");
    let output_ids = fixtures.join("struct_output_ids.txt");

    gzip_file(&input_uc, &input_gz);

    let output = Command::new(binary_path())
        .args([
            input_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "10",
        ])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());

    // Read and verify TSV structure
    let lines = read_gzipped_lines(&output_tsv);

    // Check header (no prefix)
    assert_eq!(
        lines[0],
        "seq_id\tcluster_id\tcluster_rep_id\tseq_length\tis_cluster_rep\tpercent_identity\torientation\tcigar\tcluster_size"
    );

    // Check row count: 3 S records + 3 H records = 6 data rows + 1 header = 7 lines
    assert_eq!(lines.len(), 7, "Expected 7 lines (1 header + 6 data rows)");

    // Check each data row has 9 columns
    for (i, line) in lines.iter().enumerate().skip(1) {
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 9, "Row {} has {} columns, expected 9", i, cols.len());
    }

    // Cleanup
    fs::remove_file(&input_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}

#[test]
fn test_output_tsv_with_prefix() {
    let fixtures = fixtures_dir();
    let input_uc = fixtures.join("tiny.uc");
    let input_gz = fixtures.join("tiny_prefix.uc.gz");
    let output_tsv = fixtures.join("prefix_output.tsv.gz");
    let output_ids = fixtures.join("prefix_output_ids.txt");

    gzip_file(&input_uc, &input_gz);

    let output = Command::new(binary_path())
        .args([
            input_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "10",
            "-p",
            "vsearch",
        ])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());

    let lines = read_gzipped_lines(&output_tsv);

    // Check header has prefix (except seq_id)
    let header_cols: Vec<&str> = lines[0].split('\t').collect();
    assert_eq!(header_cols[0], "seq_id");
    assert_eq!(header_cols[1], "vsearch_cluster_id");
    assert_eq!(header_cols[4], "vsearch_is_cluster_rep");

    // Cleanup
    fs::remove_file(&input_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}

#[test]
fn test_boolean_format() {
    let fixtures = fixtures_dir();
    let input_uc = fixtures.join("tiny.uc");
    let input_gz = fixtures.join("tiny_bool.uc.gz");
    let output_tsv = fixtures.join("bool_output.tsv.gz");
    let output_ids = fixtures.join("bool_output_ids.txt");

    gzip_file(&input_uc, &input_gz);

    let output = Command::new(binary_path())
        .args([
            input_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "10",
        ])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());

    let lines = read_gzipped_lines(&output_tsv);

    // Find is_cluster_rep column values
    let mut found_true = false;
    let mut found_false = false;

    for line in lines.iter().skip(1) {
        let cols: Vec<&str> = line.split('\t').collect();
        let is_rep = cols[4];
        if is_rep == "True" {
            found_true = true;
        } else if is_rep == "False" {
            found_false = true;
        } else {
            panic!("Unexpected boolean value: {}", is_rep);
        }
    }

    assert!(found_true, "Expected to find 'True' values");
    assert!(found_false, "Expected to find 'False' values");

    // Cleanup
    fs::remove_file(&input_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}

#[test]
fn test_top_n_ids_output() {
    let fixtures = fixtures_dir();
    let input_uc = fixtures.join("tiny.uc");
    let input_gz = fixtures.join("tiny_ids.uc.gz");
    let output_tsv = fixtures.join("ids_output.tsv.gz");
    let output_ids = fixtures.join("ids_output_ids.txt");

    gzip_file(&input_uc, &input_gz);

    // Request top 2 clusters
    let output = Command::new(binary_path())
        .args([
            input_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "2",
        ])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());

    let ids = read_lines(&output_ids);

    // Should have exactly 2 IDs
    assert_eq!(ids.len(), 2, "Expected 2 representative IDs");

    // Cluster sizes: alpha=3, beta=2, gamma=1
    // Top 2 by size: alpha (3), beta (2)
    assert_eq!(ids[0], "rep_alpha", "Largest cluster rep should be first");
    assert_eq!(ids[1], "rep_beta", "Second largest cluster rep should be second");

    // Cleanup
    fs::remove_file(&input_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}

#[test]
fn test_top_n_larger_than_clusters() {
    let fixtures = fixtures_dir();
    let input_uc = fixtures.join("tiny.uc");
    let input_gz = fixtures.join("tiny_large_n.uc.gz");
    let output_tsv = fixtures.join("large_n_output.tsv.gz");
    let output_ids = fixtures.join("large_n_output_ids.txt");

    gzip_file(&input_uc, &input_gz);

    // Request top 100 clusters (more than exist)
    let output = Command::new(binary_path())
        .args([
            input_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "100",
        ])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());

    let ids = read_lines(&output_ids);

    // Should have all 3 clusters (not 100)
    assert_eq!(ids.len(), 3, "Should output all available clusters when n > total");

    // Cleanup
    fs::remove_file(&input_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}

#[test]
fn test_error_on_malformed_input() {
    let fixtures = fixtures_dir();
    let malformed_uc = fixtures.join("malformed.uc");
    let malformed_gz = fixtures.join("malformed.uc.gz");
    let output_tsv = fixtures.join("malformed_output.tsv.gz");
    let output_ids = fixtures.join("malformed_output_ids.txt");

    // Create malformed input (wrong number of fields)
    fs::write(&malformed_uc, "S\t0\t100\tonly_four_fields\n").expect("Failed to write malformed file");
    gzip_file(&malformed_uc, &malformed_gz);

    let output = Command::new(binary_path())
        .args([
            malformed_gz.to_str().unwrap(),
            output_tsv.to_str().unwrap(),
            output_ids.to_str().unwrap(),
            "-n",
            "10",
        ])
        .output()
        .expect("Failed to execute binary");

    // Should fail
    assert!(!output.status.success(), "Should fail on malformed input");

    // Error message should mention field count
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("expected") && stderr.contains("fields"),
        "Error should mention field count: {}",
        stderr
    );

    // Cleanup
    fs::remove_file(&malformed_uc).ok();
    fs::remove_file(&malformed_gz).ok();
    fs::remove_file(&output_tsv).ok();
    fs::remove_file(&output_ids).ok();
}
