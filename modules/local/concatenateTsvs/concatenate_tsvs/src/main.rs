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

fn check_headers(header: &[String], reference_header: &[String]) -> Result<(), String> {
    use std::collections::HashSet;
    
    let hset: HashSet<&String> = header.iter().collect();
    let rset: HashSet<&String> = reference_header.iter().collect();
    
    let header_difference: Vec<_> = hset.symmetric_difference(&rset).collect();
    
    if header_difference.is_empty() {
        return Ok(());
    }
    
    let mut msg = "Headers do not match:".to_string();
    let missing: Vec<_> = rset.difference(&hset).collect();
    if !missing.is_empty() {
        msg.push_str(&format!("\n\tMissing fields: {:?}", missing));
    }
    let extra: Vec<_> = hset.difference(&rset).collect();
    if !extra.is_empty() {
        msg.push_str(&format!("\n\tExtra fields: {:?}", extra));
    }
    
    Err(msg)
}

fn map_headers(header: &[String], reference_header: &[String]) -> Vec<usize> {
    let header_mapping: std::collections::HashMap<&String, usize> = 
        header.iter().enumerate().map(|(i, col)| (col, i)).collect();
    
    reference_header
        .iter()
        .map(|col| *header_mapping.get(col).expect("Header field not found in mapping"))
        .collect()
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
            // Check headers match (fields must match, order can differ)
            check_headers(&header, ref_header)?;

            // Generate mapping of header fields to reference header
            let header_mapping = map_headers(&header, ref_header);

            // Write all data lines with mapped fields
            let mut line = String::new();
            while reader.read_line(&mut line)? > 0 {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    line.clear();
                    continue;
                }
                
                let fields: Vec<&str> = trimmed.split('\t').collect();
                let mapped_fields: Vec<&str> = header_mapping
                    .iter()
                    .map(|&idx| fields.get(idx).copied().unwrap_or(""))
                    .collect();
                
                writeln!(writer, "{}", mapped_fields.join("\t"))?;
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
