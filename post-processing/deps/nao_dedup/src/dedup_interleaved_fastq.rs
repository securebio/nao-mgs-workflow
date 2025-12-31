use clap::Parser;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use nao_dedup::{DedupContext, DedupParams, MinimizerParams};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dedup_interleaved_fastq")]
#[command(about = "Deduplicate interleaved paired-end FASTQ files", long_about = None)]
struct Cli {
    /// Input FASTQ.gz file (interleaved R1/R2)
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Output FASTQ.gz file (exemplars only)
    #[arg(value_name = "OUTPUT")]
    output: PathBuf,

    /// Maximum alignment offset (default: 1)
    #[arg(long, default_value_t = 1)]
    max_offset: usize,

    /// Maximum error fraction (default: 0.01)
    #[arg(long, default_value_t = 0.01)]
    max_error_frac: f64,

    /// K-mer length for minimizers (default: 15)
    #[arg(long, default_value_t = 15)]
    kmer_len: usize,

    /// Window length for minimizers (default: 25)
    #[arg(long, default_value_t = 25)]
    window_len: usize,

    /// Number of windows for minimizers (default: 4)
    #[arg(long, default_value_t = 4)]
    num_windows: usize,
}

#[derive(Debug)]
struct FastqRecord {
    header: String,
    sequence: String,
    plus: String,
    quality: String,
}

impl FastqRecord {
    fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writeln!(writer, "{}", self.header)?;
        writeln!(writer, "{}", self.sequence)?;
        writeln!(writer, "{}", self.plus)?;
        writeln!(writer, "{}", self.quality)?;
        Ok(())
    }
}

fn read_fastq_record<R: BufRead>(reader: &mut R) -> std::io::Result<Option<FastqRecord>> {
    let mut header = String::new();
    let mut sequence = String::new();
    let mut plus = String::new();
    let mut quality = String::new();

    // Read header
    if reader.read_line(&mut header)? == 0 {
        return Ok(None);
    }
    header.truncate(header.trim_end().len());

    // Validate header starts with '@'
    if !header.starts_with('@') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid FASTQ header: expected '@', got '{}'", header),
        ));
    }

    // Read sequence
    if reader.read_line(&mut sequence)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete FASTQ record: missing sequence",
        ));
    }
    sequence.truncate(sequence.trim_end().len());

    // Read + line
    if reader.read_line(&mut plus)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete FASTQ record: missing plus line",
        ));
    }
    plus.truncate(plus.trim_end().len());

    // Validate separator is exactly '+'
    if plus != "+" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid FASTQ separator: expected '+', got '{}'", plus),
        ));
    }

    // Read quality
    if reader.read_line(&mut quality)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete FASTQ record: missing quality",
        ));
    }
    quality.truncate(quality.trim_end().len());

    Ok(Some(FastqRecord {
        header,
        sequence,
        plus,
        quality,
    }))
}

/// Iterator that yields pairs of FASTQ records from an interleaved FASTQ file.
struct FastqPairIterator<R: BufRead> {
    reader: R,
}

impl<R: BufRead> FastqPairIterator<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: BufRead> Iterator for FastqPairIterator<R> {
    type Item = std::io::Result<(FastqRecord, FastqRecord)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read R1
        let r1 = match read_fastq_record(&mut self.reader) {
            Ok(Some(record)) => record,
            Ok(None) => return None, // EOF
            Err(e) => return Some(Err(e)),
        };

        // Read R2
        let r2 = match read_fastq_record(&mut self.reader) {
            Ok(Some(record)) => record,
            Ok(None) => {
                eprintln!("Warning: Odd number of reads in file. Last read ignored.");
                return None;
            }
            Err(e) => return Some(Err(e)),
        };

        Some(Ok((r1, r2)))
    }
}

/// Creates a FASTQ pair iterator from a gzipped file.
fn create_pair_iterator(
    path: &PathBuf,
) -> std::io::Result<FastqPairIterator<BufReader<GzDecoder<File>>>> {
    let input_file = File::open(path)?;
    let gz_decoder = GzDecoder::new(input_file);
    let reader = BufReader::new(gz_decoder);
    Ok(FastqPairIterator::new(reader))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Set up parameters
    let dedup_params = DedupParams {
        max_offset: cli.max_offset,
        max_error_frac: cli.max_error_frac,
    };

    let minimizer_params = MinimizerParams {
        kmer_len: cli.kmer_len,
        window_len: cli.window_len,
        num_windows: cli.num_windows,
    };

    eprintln!("Pass 1: Building deduplication index...");

    // Pass 1: Read all pairs and build deduplication index
    let pair_iter = create_pair_iterator(&cli.input)?;

    let mut ctx = DedupContext::new(dedup_params, minimizer_params);
    let mut pair_count = 0;

    for (idx, pair_result) in pair_iter.enumerate() {
        let (r1, r2) = pair_result?;

        // Process by index directly (more efficient than creating ReadPair with string ID)
        ctx.process_read_by_index(
            idx,
            r1.sequence,
            r2.sequence,
            r1.quality,
            r2.quality,
        );
        pair_count = idx + 1;

        if pair_count % 100_000 == 0 {
            eprintln!("  Processed {} read pairs...", pair_count);
        }
    }

    eprintln!("  Total read pairs: {}", pair_count);

    // Finalize deduplication
    eprintln!("Finalizing deduplication...");
    ctx.finalize();

    let (total_reads, unique_clusters) = ctx.stats();
    eprintln!("  Total reads: {}", total_reads);
    eprintln!("  Unique clusters: {}", unique_clusters);

    if total_reads > 0 {
        eprintln!(
            "  Deduplication rate: {:.2}%",
            (1.0 - unique_clusters as f64 / total_reads as f64) * 100.0
        );
    } else {
        eprintln!("Warning: No reads found in input file");
    }

    // Build set of exemplar indices
    let exemplar_indices = ctx.get_exemplar_indices();

    eprintln!("Pass 2: Writing exemplars to output...");

    // Pass 2: Write exemplars
    let pair_iter = create_pair_iterator(&cli.input)?;

    let output_file = File::create(&cli.output)?;
    let gz_encoder = GzEncoder::new(output_file, Compression::default());
    let mut writer = BufWriter::new(gz_encoder);

    let mut written = 0;

    for (idx, pair_result) in pair_iter.enumerate() {
        let (r1, r2) = pair_result?;

        // Write if this is an exemplar
        if exemplar_indices.contains(&idx) {
            r1.write_to(&mut writer)?;
            r2.write_to(&mut writer)?;
            written += 1;
        }

        let current_index = idx + 1;
        if current_index % 100_000 == 0 {
            eprintln!("  Processed {} read pairs...", current_index);
        }
    }

    eprintln!("  Wrote {} exemplar pairs", written);
    eprintln!("Done!");

    Ok(())
}
