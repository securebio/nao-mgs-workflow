use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::Command;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

// ------------------------------------------------------------------------------------------------
// HELPERS
// ------------------------------------------------------------------------------------------------

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mark_duplicates_similarity"))
}

fn gzip_content(content: &str, path: &PathBuf) {
    let file = File::create(path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(content.as_bytes()).unwrap();
}

fn read_gzipped_lines(path: &PathBuf) -> Vec<String> {
    let file = File::open(path).unwrap();
    BufReader::new(GzDecoder::new(file))
        .lines()
        .map(|l| l.unwrap())
        .collect()
}

/// Parse gzipped TSV into header + list of row dicts
fn read_gzipped_tsv(path: &PathBuf) -> (Vec<String>, Vec<HashMap<String, String>>) {
    let lines = read_gzipped_lines(path);
    if lines.is_empty() {
        return (vec![], vec![]);
    }
    let header: Vec<String> = lines[0].split('\t').map(|s| s.to_string()).collect();
    let rows: Vec<HashMap<String, String>> = lines[1..]
        .iter()
        .map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            header
                .iter()
                .enumerate()
                .map(|(i, col)| (col.clone(), fields.get(i).unwrap_or(&"").to_string()))
                .collect()
        })
        .collect();
    (header, rows)
}

struct TestFiles {
    input_gz: PathBuf,
    output_gz: PathBuf,
}

impl TestFiles {
    fn new(prefix: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("sim_dup_test_{}", prefix));
        std::fs::create_dir_all(&dir).unwrap();
        Self {
            input_gz: dir.join("input.tsv.gz"),
            output_gz: dir.join("output.tsv.gz"),
        }
    }

    fn write_input(&self, content: &str) {
        gzip_content(content, &self.input_gz);
    }

    fn run(&self) -> std::process::Output {
        Command::new(binary_path())
            .args([
                "-i",
                self.input_gz.to_str().unwrap(),
                "-o",
                self.output_gz.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    }

    fn run_and_parse(&self) -> (Vec<String>, Vec<HashMap<String, String>>) {
        let output = self.run();
        assert!(
            output.status.success(),
            "Binary failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        read_gzipped_tsv(&self.output_gz)
    }
}

impl Drop for TestFiles {
    fn drop(&mut self) {
        // Clean up the temp directory
        if let Some(dir) = self.input_gz.parent() {
            std::fs::remove_dir_all(dir).ok();
        }
    }
}

/// Build TSV content from rows of (seq_id, seq, qual, prim_align_exemplar).
/// Sequences are used for both fwd and rev; qualities likewise.
fn build_tsv(rows: &[(&str, &str, &str, &str)]) -> String {
    let mut lines = vec![
        "seq_id\tquery_seq\tquery_seq_rev\tquery_qual\tquery_qual_rev\tprim_align_dup_exemplar"
            .to_string(),
    ];
    for (seq_id, seq, qual, exemplar) in rows {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            seq_id, seq, seq, qual, qual, exemplar
        ));
    }
    lines.join("\n") + "\n"
}

// ------------------------------------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------------------------------------

#[test]
fn test_no_duplicates() {
    let seq1 = "A".repeat(76);
    let seq2 = "G".repeat(76);
    let qual1 = "I".repeat(76);
    let qual2 = "H".repeat(76);

    let files = TestFiles::new("no_dup");
    files.write_input(&build_tsv(&[
        ("read1", &seq1, &qual1, "read1"),
        ("read2", &seq2, &qual2, "read2"),
    ]));

    let (_header, rows) = files.run_and_parse();

    // Each read should be its own similarity exemplar
    assert_eq!(rows[0]["sim_dup_exemplar"], "read1");
    assert_eq!(rows[1]["sim_dup_exemplar"], "read2");
    // Each is an exemplar with no duplicates, so group_size = 1
    assert_eq!(rows[0]["sim_dup_group_size"], "1");
    assert_eq!(rows[1]["sim_dup_group_size"], "1");
}

#[test]
fn test_alignment_duplicates_get_na() {
    let seq1 = "ACGT".repeat(19);
    let seq2 = "TGCA".repeat(19);
    let seq3 = "GGCC".repeat(19);
    let qual = "I".repeat(76);

    let files = TestFiles::new("align_dup");
    files.write_input(&build_tsv(&[
        ("read1", &seq1, &qual, "read1"),
        ("read2", &seq2, &qual, "read1"), // alignment duplicate of read1
        ("read3", &seq3, &qual, "read3"),
    ]));

    let (_header, rows) = files.run_and_parse();

    // read1 and read3 are alignment-unique, should have sim_dup_exemplar
    assert_eq!(rows[0]["sim_dup_exemplar"], "read1");
    assert_eq!(rows[2]["sim_dup_exemplar"], "read3");
    // read2 is alignment duplicate, should have 'NA'
    assert_eq!(rows[1]["sim_dup_exemplar"], "NA");
    // read1 has 2 reads in its alignment group (read1 + read2), read3 has 1
    assert_eq!(rows[0]["sim_dup_group_size"], "2");
    assert_eq!(rows[2]["sim_dup_group_size"], "1");
    // Alignment dup gets NA
    assert_eq!(rows[1]["sim_dup_group_size"], "NA");
}

#[test]
fn test_similarity_duplicates() {
    // read1 and read3 are identical sequences (similarity duplicates)
    let seq1 = "A".repeat(76);
    let seq2 = "C".repeat(76);
    let qual1 = "I".repeat(76);
    let qual2 = "H".repeat(76);

    let files = TestFiles::new("sim_dup");
    files.write_input(&build_tsv(&[
        ("read1", &seq1, &qual1, "read1"),
        ("read2", &seq2, &qual2, "read1"), // alignment duplicate of read1
        ("read3", &seq1, &qual1, "read3"), // identical to read1
    ]));

    let (_header, rows) = files.run_and_parse();

    // read1 and read3 are alignment-unique and similar
    // They should have the same sim_dup_exemplar
    // (identical quality scores, so either could be chosen)
    let exemplar = &rows[0]["sim_dup_exemplar"];
    assert_eq!(&rows[2]["sim_dup_exemplar"], exemplar);
    // read2 is alignment duplicate, should have 'NA'
    assert_eq!(rows[1]["sim_dup_exemplar"], "NA");
    // The sim exemplar gets total group size:
    // read1 has 2 alignment reads (read1 + read2), read3 has 1, total = 3
    if exemplar == "read1" {
        assert_eq!(rows[0]["sim_dup_group_size"], "3");
        assert_eq!(rows[2]["sim_dup_group_size"], "NA");
    } else {
        assert_eq!(rows[2]["sim_dup_group_size"], "3");
        assert_eq!(rows[0]["sim_dup_group_size"], "NA");
    }
    // Alignment dup always gets NA
    assert_eq!(rows[1]["sim_dup_group_size"], "NA");
}

#[test]
fn test_empty_file() {
    let files = TestFiles::new("empty");
    files.write_input(&build_tsv(&[]));

    let (header, rows) = files.run_and_parse();

    assert_eq!(rows.len(), 0);
    assert!(header.contains(&"sim_dup_exemplar".to_string()));
    assert!(header.contains(&"sim_dup_group_size".to_string()));
}

#[test]
fn test_single_read() {
    let seq = "ACGT".repeat(12);
    let qual = "I".repeat(48);

    let files = TestFiles::new("single");
    files.write_input(&build_tsv(&[("read1", &seq, &qual, "read1")]));

    let (_header, rows) = files.run_and_parse();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["sim_dup_exemplar"], "read1");
    assert_eq!(rows[0]["sim_dup_group_size"], "1");
}

#[test]
fn test_column_order_preserved() {
    let seq = "ACGT".repeat(12);
    let qual = "I".repeat(48);

    // Manually create content with extra columns in non-standard order
    let content = format!(
        "extra1\tseq_id\textra2\tquery_seq\tquery_seq_rev\t\
         query_qual\tquery_qual_rev\tprim_align_dup_exemplar\textra3\n\
         val1\tread1\tval2\t{seq}\t{seq}\t{qual}\t{qual}\tread1\tval3\n"
    );

    let files = TestFiles::new("col_order");
    files.write_input(&content);

    let (header, rows) = files.run_and_parse();

    // Check column order: original columns + sim_dup_exemplar + sim_dup_group_size at end
    let expected = vec![
        "extra1",
        "seq_id",
        "extra2",
        "query_seq",
        "query_seq_rev",
        "query_qual",
        "query_qual_rev",
        "prim_align_dup_exemplar",
        "extra3",
        "sim_dup_exemplar",
        "sim_dup_group_size",
    ];
    assert_eq!(header, expected);

    // Check extra columns preserved
    assert_eq!(rows[0]["extra1"], "val1");
    assert_eq!(rows[0]["extra2"], "val2");
    assert_eq!(rows[0]["extra3"], "val3");
}

#[test]
fn test_all_alignment_duplicates() {
    let seq = "ACGT".repeat(12);
    let qual = "I".repeat(48);

    let files = TestFiles::new("all_align");
    files.write_input(&build_tsv(&[
        ("read1", &seq, &qual, "read1"),
        ("read2", &seq, &qual, "read1"), // alignment dup
        ("read3", &seq, &qual, "read1"), // alignment dup
    ]));

    let (_header, rows) = files.run_and_parse();

    // Only read1 is alignment-unique
    assert_eq!(rows[0]["sim_dup_exemplar"], "read1");
    assert_eq!(rows[1]["sim_dup_exemplar"], "NA");
    assert_eq!(rows[2]["sim_dup_exemplar"], "NA");
    // read1 is the sim exemplar with all 3 reads in its alignment group
    assert_eq!(rows[0]["sim_dup_group_size"], "3");
    assert_eq!(rows[1]["sim_dup_group_size"], "NA");
    assert_eq!(rows[2]["sim_dup_group_size"], "NA");
}

#[test]
fn test_multi_level_group_sizes() {
    // Two alignment-unique reads (read1, read3) that are similarity duplicates.
    // read1 has higher quality so should be chosen as the sim exemplar.
    let seq = "A".repeat(76);
    let qual_high = "I".repeat(76);
    let qual_low = "5".repeat(76);

    let files = TestFiles::new("multi_level");
    files.write_input(&build_tsv(&[
        // Alignment group 1: read1 (exemplar) + read2 (align dup)
        ("read1", &seq, &qual_high, "read1"),
        ("read2", &seq, &qual_low, "read1"),
        // Alignment group 2: read3 (exemplar) + read4, read5 (align dups)
        ("read3", &seq, &qual_low, "read3"),
        ("read4", &seq, &qual_low, "read3"),
        ("read5", &seq, &qual_low, "read3"),
    ]));

    let (header, rows) = files.run_and_parse();

    assert!(header.contains(&"sim_dup_group_size".to_string()));

    // read1 has higher quality, so it should be the sim exemplar
    assert_eq!(rows[0]["sim_dup_exemplar"], "read1");
    assert_eq!(rows[2]["sim_dup_exemplar"], "read1");
    // Alignment dups always get NA
    assert_eq!(rows[1]["sim_dup_exemplar"], "NA");
    assert_eq!(rows[3]["sim_dup_exemplar"], "NA");
    assert_eq!(rows[4]["sim_dup_exemplar"], "NA");

    // The sim exemplar gets total count: 2 (read1 group) + 3 (read3 group) = 5
    assert_eq!(rows[0]["sim_dup_group_size"], "5");
    // Sim dup and alignment dups all get NA
    assert_eq!(rows[1]["sim_dup_group_size"], "NA");
    assert_eq!(rows[2]["sim_dup_group_size"], "NA");
    assert_eq!(rows[3]["sim_dup_group_size"], "NA");
    assert_eq!(rows[4]["sim_dup_group_size"], "NA");
}
