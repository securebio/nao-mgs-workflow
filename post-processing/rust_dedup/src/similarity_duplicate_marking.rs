use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use nao_dedup::{DedupContext, DedupParams, MinimizerParams, ReadPair};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

fn find_column(header_fields: &[&str], name: &str) -> Result<usize> {
    header_fields
        .iter()
        .position(|&f| f == name)
        .with_context(|| format!("Missing required column: {}", name))
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        bail!("Usage: {} <input.tsv.gz> <output.tsv.gz>", args[0]);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    let start_time = Instant::now();

    // Create deduplication context with default parameters
    let dedup_params = DedupParams::default();
    let minimizer_params = MinimizerParams::default();
    let mut ctx = DedupContext::new(dedup_params, minimizer_params);

    // Pass 1: Process alignment-unique reads
    let mut n_reads = 0;
    let mut alignment_unique_count = 0;

    eprintln!("Running similarity-based deduplication on alignment-unique reads...");

    let file = File::open(input_path)
        .with_context(|| format!("Cannot open input file: {}", input_path))?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);
    let mut lines = reader.lines();

    // Read header
    let header = lines
        .next()
        .context("Empty input file")?
        .context("Failed to read header")?;

    let header_fields: Vec<&str> = header.split('\t').collect();

    // Find column indices
    let seq_id_idx = find_column(&header_fields, "seq_id")?;
    let query_seq_idx = find_column(&header_fields, "query_seq")?;
    let query_seq_rev_idx = find_column(&header_fields, "query_seq_rev")?;
    let query_qual_idx = find_column(&header_fields, "query_qual")?;
    let query_qual_rev_idx = find_column(&header_fields, "query_qual_rev")?;
    let prim_align_idx = find_column(&header_fields, "prim_align_dup_exemplar")?;

    // Minimum number of fields required to access all required columns
    let min_fields = [
        seq_id_idx,
        query_seq_idx,
        query_seq_rev_idx,
        query_qual_idx,
        query_qual_rev_idx,
        prim_align_idx,
    ]
    .into_iter()
    .max()
    .unwrap()
        + 1;

    // Process reads
    for line_result in lines {
        let line = line_result.context("Failed to read line")?;
        n_reads += 1;

        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < min_fields {
            bail!(
                "Malformed line {}: expected at least {} fields, got {}",
                n_reads,
                min_fields,
                fields.len()
            );
        }

        let seq_id = fields[seq_id_idx];
        let prim_align_exemplar = fields[prim_align_idx];

        // Only process alignment-unique reads
        if seq_id != prim_align_exemplar {
            continue;
        }

        alignment_unique_count += 1;

        let read_pair = ReadPair {
            read_id: seq_id.to_string(),
            fwd_seq: fields[query_seq_idx].to_string(),
            rev_seq: fields[query_seq_rev_idx].to_string(),
            fwd_qual: fields[query_qual_idx].to_string(),
            rev_qual: fields[query_qual_rev_idx].to_string(),
        };

        ctx.process_read(read_pair);
    }

    let (_total_processed, unique_clusters) = ctx.stats();
    eprintln!(
        "Processed {} alignment-unique reads (out of {} total reads)",
        alignment_unique_count, n_reads
    );
    eprintln!("Found {} unique sequence clusters", unique_clusters);

    // Finalize Pass 1
    ctx.finalize();

    // Pass 2: Write output with sim_dup_exemplar column
    eprintln!("Pass 2: Writing output with sim_dup_exemplar column...");

    let file_in = File::open(input_path)
        .with_context(|| format!("Cannot open input file: {}", input_path))?;
    let decoder = GzDecoder::new(file_in);
    let reader = BufReader::new(decoder);
    let mut lines = reader.lines();

    let file_out = File::create(output_path)
        .with_context(|| format!("Cannot create output file: {}", output_path))?;
    let encoder = GzEncoder::new(file_out, Compression::default());
    let mut writer = BufWriter::new(encoder);

    // Skip header line and write stored header with new column
    lines.next();
    writeln!(writer, "{}\tsim_dup_exemplar", header.trim_end())
        .context("Failed to write header")?;

    let mut n_prim_align_dups = 0;
    let mut n_sim_dups = 0;

    // Process data rows
    for (line_num, line_result) in lines.enumerate() {
        let line = line_result.context("Failed to read line")?;
        let fields: Vec<&str> = line.split('\t').collect();

        if fields.len() < min_fields {
            bail!(
                "Malformed line {}: expected at least {} fields, got {}",
                line_num + 1,
                min_fields,
                fields.len()
            );
        }

        let seq_id = fields[seq_id_idx];
        let prim_align_exemplar = fields[prim_align_idx];

        if seq_id != prim_align_exemplar {
            // Alignment duplicate - fast path
            writeln!(writer, "{}\tNA", line.trim_end()).context("Failed to write line")?;
            n_prim_align_dups += 1;
        } else {
            // Alignment-unique - query for similarity exemplar
            let sim_exemplar = ctx.get_cluster_id(seq_id);
            writeln!(writer, "{}\t{}", line.trim_end(), sim_exemplar)
                .context("Failed to write line")?;

            if sim_exemplar != seq_id {
                n_sim_dups += 1;
            }
        }
    }

    writer.flush().context("Failed to flush output")?;

    let elapsed = start_time.elapsed();
    eprintln!("Done!");
    eprintln!(
        "Marked similarity duplicates processing {} reads in {}s, of which {} \
        were already non-exemplars via alignment and {} were additionally \
        recognized as non-exemplars via similarity.",
        n_reads, elapsed.as_secs(), n_prim_align_dups, n_sim_dups
    );

    Ok(())
}
