use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use flate2::{Compression as GzCompression, write::GzEncoder, read::GzDecoder};
use bzip2::{Compression as BzCompression, write::BzEncoder, read::BzDecoder};
use clap::Parser;

// ------------------------------------------------------------------------------------------------
// ARGUMENT PARSING
// ------------------------------------------------------------------------------------------------

/// Concatenate multiple TSV files with matching headers
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input TSV file paths
    #[arg(short, long, num_args(1..))]
    input_files: Vec<String>,
    /// Output TSV file path
    #[arg(short = 'o', long)]
    output_file: String,
}

// Define a reader based on the file extension
fn open_reader(filename: &str) -> std::io::Result<Box<dyn BufRead>> {
    let file = File::open(filename)?;
    if filename.ends_with(".gz") {
        let decoder = GzDecoder::new(file);
        Ok(Box::new(BufReader::new(decoder)))
    } else if filename.ends_with(".bz2") {
        let decoder = BzDecoder::new(file);
        Ok(Box::new(BufReader::new(decoder)))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

// Define a writer based on the file extension
fn open_writer(filename: &str) -> std::io::Result<Box<dyn Write>> {
    if filename.ends_with(".gz") {
        let file = File::create(filename)?;
        let encoder = GzEncoder::new(file, GzCompression::default());
        Ok(Box::new(BufWriter::new(encoder)))
    } else if filename.ends_with(".bz2") {
        let file = File::create(filename)?;
        let encoder = BzEncoder::new(file, BzCompression::default());
        Ok(Box::new(BufWriter::new(encoder)))
    } else {
        let file = File::create(filename)?;
        Ok(Box::new(BufWriter::new(file)))
    }
}

fn read_header(reader: &mut dyn BufRead) -> io::Result<Option<Vec<String>>> {
    let mut line = String::new();
    match reader.read_line(&mut line)? {
        0 => Ok(None), // Empty file
        _ => {
            let header: Vec<String> = line.trim().split('\t').map(|s| s.to_string()).collect();
            if header.is_empty() || header.iter().all(|s| s.is_empty()) {
                Ok(None)
            } else {
                Ok(Some(header))
            }
        }
    }
}

fn check_headers_match(header1: &[String], header2: &[String]) -> Result<(), String> {
    if header1.len() != header2.len() {
        return Err("Header length mismatch".to_string());
    }

    for (i, (h1, h2)) in header1.iter().zip(header2.iter()).enumerate() {
        if h1 != h2 {
            return Err("Headers do not match".to_string());
        }
    }

    Ok(())
}

fn concatenate_tsvs(input_files: &[String], output_file: &str) -> Result<(), Box<dyn Error>> {
    let mut writer = open_writer(output_file)?;

    // Find first non-empty file to get reference header
    let mut reference_header: Option<Vec<String>> = None;
    let mut first_valid_index = None;

    for (idx, input_path) in input_files.iter().enumerate() {
        let mut reader = open_reader(input_path)?;

        if let Some(header) = read_header(&mut *reader)? {
            reference_header = Some(header.clone());
            first_valid_index = Some(idx);

            // Write header to output
            writeln!(writer, "{}", header.join("\t"))?;

            // Write all lines from first valid file
            let mut line = String::new();
            while reader.read_line(&mut line)? > 0 {
                writer.write_all(line.as_bytes())?;
                line.clear();
            }

            break;
        }
    }

    // If no valid files found, create empty output
    let Some(ref ref_header) = reference_header else {
        eprintln!("Warning: All input files are empty. Creating empty output file.");
        return Ok(());
    };

    let Some(first_idx) = first_valid_index else {
        return Ok(());
    };

    // Process remaining files
    for input_path in input_files.iter().skip(first_idx + 1) {
        let mut reader = open_reader(input_path)?;

        if let Some(header) = read_header(&mut *reader)? {
            // Check headers match
            check_headers_match(&header, ref_header)?;

            // Write all data lines (skip header)
            let mut line = String::new();
            while reader.read_line(&mut line)? > 0 {
                writer.write_all(line.as_bytes())?;
                line.clear();
            }
        }
        // Empty files are silently skipped
    }

    writer.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args = Args::parse();
    // Concatenate TSVs
    return concatenate_tsvs(&args.input_files, &args.output_file);
}
