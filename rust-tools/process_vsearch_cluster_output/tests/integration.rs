use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::Command;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_process_vsearch_cluster_output"))
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn gzip_content(content: &str, output_path: &PathBuf) {
    let file = File::create(output_path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(content.as_bytes()).unwrap();
}

fn read_gzipped_lines(path: &PathBuf) -> Vec<String> {
    let file = File::open(path).unwrap();
    BufReader::new(GzDecoder::new(file)).lines().map(|l| l.unwrap()).collect()
}

fn read_lines(path: &PathBuf) -> Vec<String> {
    BufReader::new(File::open(path).unwrap()).lines().map(|l| l.unwrap()).collect()
}

struct TestFiles {
    input_gz: PathBuf,
    output_tsv: PathBuf,
    output_ids: PathBuf,
}

impl TestFiles {
    fn new(prefix: &str) -> Self {
        Self::with_content(prefix, &fs::read_to_string(fixtures_dir().join("tiny.uc")).unwrap())
    }

    fn with_content(prefix: &str, content: &str) -> Self {
        let fixtures = fixtures_dir();
        let input_gz = fixtures.join(format!("{}.uc.gz", prefix));
        gzip_content(content, &input_gz);
        Self {
            input_gz,
            output_tsv: fixtures.join(format!("{}_output.tsv.gz", prefix)),
            output_ids: fixtures.join(format!("{}_output_ids.txt", prefix)),
        }
    }
}

impl Drop for TestFiles {
    fn drop(&mut self) {
        fs::remove_file(&self.input_gz).ok();
        fs::remove_file(&self.output_tsv).ok();
        fs::remove_file(&self.output_ids).ok();
    }
}

#[test]
fn test_happy_path() {
    let files = TestFiles::new("happy");

    let output = Command::new(binary_path())
        .args([
            files.input_gz.to_str().unwrap(),
            files.output_tsv.to_str().unwrap(),
            files.output_ids.to_str().unwrap(),
            "-n", "2",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Failed: {}", String::from_utf8_lossy(&output.stderr));

    // Verify TSV output
    let lines = read_gzipped_lines(&files.output_tsv);

    // Check header
    assert_eq!(lines[0],
        "seq_id\tcluster_id\tcluster_rep_id\tseq_length\tis_cluster_rep\tpercent_identity\torientation\tcigar\tcluster_size");

    // Check row count: 3 S records + 3 H records = 6 data rows + 1 header
    assert_eq!(lines.len(), 7);

    // Check column count and boolean format
    // Booleans should be "True" or "False", matching Python, and we should have
    // both values (both H and S records)
    let mut found_true = false;
    let mut found_false = false;
    for line in lines.iter().skip(1) {
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 9);
        match cols[4] {
            "True" => found_true = true,
            "False" => found_false = true,
            other => panic!("Unexpected boolean: {}", other),
        }
    }
    assert!(found_true && found_false, "Should have both True and False values");

    // Verify top-N IDs output (cluster sizes: alpha=3, beta=2, gamma=1)
    let ids = read_lines(&files.output_ids);
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], "rep_alpha");
    assert_eq!(ids[1], "rep_beta");
}

#[test]
fn test_with_prefix() {
    let files = TestFiles::new("prefix");

    let output = Command::new(binary_path())
        .args([
            files.input_gz.to_str().unwrap(),
            files.output_tsv.to_str().unwrap(),
            files.output_ids.to_str().unwrap(),
            "-n", "10",
            "-p", "vsearch",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());

    let lines = read_gzipped_lines(&files.output_tsv);
    let header_cols: Vec<&str> = lines[0].split('\t').collect();
    assert_eq!(header_cols[0], "seq_id");
    assert_eq!(header_cols[1], "vsearch_cluster_id");
    assert_eq!(header_cols[4], "vsearch_is_cluster_rep");
}

#[test]
fn test_empty_input() {
    let files = TestFiles::with_content("empty", "");

    let output = Command::new(binary_path())
        .args([
            files.input_gz.to_str().unwrap(),
            files.output_tsv.to_str().unwrap(),
            files.output_ids.to_str().unwrap(),
            "-n", "10",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Failed on empty input: {}", String::from_utf8_lossy(&output.stderr));

    // TSV should have header only
    let lines = read_gzipped_lines(&files.output_tsv);
    assert_eq!(lines.len(), 1, "Should have header only");
    assert!(lines[0].starts_with("seq_id\t"));

    // IDs file should be empty
    let ids = read_lines(&files.output_ids);
    assert_eq!(ids.len(), 0, "Should have no IDs");
}

#[test]
fn test_error_on_malformed_input() {
    let files = TestFiles::with_content("malformed", "S\t0\t100\tonly_four_fields\n");

    let output = Command::new(binary_path())
        .args([
            files.input_gz.to_str().unwrap(),
            files.output_tsv.to_str().unwrap(),
            files.output_ids.to_str().unwrap(),
            "-n", "10",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("expected") && stderr.contains("fields"), "Error: {}", stderr);
}
