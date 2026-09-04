// ------------------------------------------------------------------------------------------------
// IMPORTS
// ------------------------------------------------------------------------------------------------

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::cmp::Ordering;
use flate2::{Compression as GzCompression, write::GzEncoder, read::GzDecoder};
use bzip2::{Compression as BzCompression, write::BzEncoder, read::BzDecoder};
use rayon::prelude::*;
use clap::Parser;

// ------------------------------------------------------------------------------------------------
// STRUCTS AND TYPES
// ------------------------------------------------------------------------------------------------

// Minimal ReadEntry struct storing only essential data for duplicate detection
#[derive(Debug, Clone)]
struct ReadEntry {
    query_name: String,
    genome_id: String,
    aln_start: Option<i32>,
    aln_end: Option<i32>,
    avg_quality: f64,
}

// Structure to store duplicate group information without storing full read data
#[derive(Debug, Clone)]
struct DuplicateGroup {
    genome_id: String,
    exemplar_name: String,
    group_size: usize,
    pairwise_match_frac: f64,
}

// Map from query_name to (genome_id, exemplar_name) for efficient lookup during second pass
type ExemplarMap = HashMap<String, (String, String)>;

// ------------------------------------------------------------------------------------------------
// ARGUMENT PARSING
// ------------------------------------------------------------------------------------------------

/// Mark duplicate reads in alignment data
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input TSV file path
    #[arg(short, long)]
    input: String,
    /// Output database file path
    #[arg(short = 'o', long)]
    output_db: String,
    /// Output metadata file path
    #[arg(short = 'm', long)]
    output_meta: String,
    /// Position deviation tolerance (0, 1, or 2)
    #[arg(short, long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    deviation: u8,
    /// Chunk size for parallel processing
    #[arg(short, long, default_value_t = 2000, value_parser = clap::value_parser!(u32).range(1..))]
    chunk_size: u32,
    /// Number of threads to use
    #[arg(short, long, default_value_t = 4, value_parser = clap::value_parser!(u8).range(1..))]
    num_threads: u8,
}

// ------------------------------------------------------------------------------------------------
// HELPER FUNCTIONS
// ------------------------------------------------------------------------------------------------

// Compare two Option<i32> positions, treating None as larger than any Some value
// This puts None values at the end of the sorted list
fn order_positions(a: Option<i32>, b: Option<i32>) -> Ordering {
    match (a, b) {
        (Some(a_pos), Some(b_pos)) => a_pos.cmp(&b_pos),
        (Some(_), None) => Ordering::Less,     // Some < None
        (None, Some(_)) => Ordering::Greater,  // None > Some
        (None, None) => Ordering::Equal,       // None == None
    }
}

// Sort ReadEntries by coordinates: first by aln_start, then by aln_end
// None values are treated as larger than any Some value (sorted to the end)
fn compare_read_coordinates(a: &ReadEntry, b: &ReadEntry) -> Ordering {
    match order_positions(a.aln_start, b.aln_start) {
        Ordering::Equal => order_positions(a.aln_end, b.aln_end),
        other => other,
    }
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

// Implement a custom match function for comparing ReadEntries
// (Not a valid equality relation as not transitive)
fn match_reads(a: &ReadEntry, b: &ReadEntry, deviation: u8) -> bool {
    a.genome_id == b.genome_id &&
    compare_positions(a.aln_start, b.aln_start, deviation) &&
    compare_positions(a.aln_end, b.aln_end, deviation)
}

// Compare the positions with a deviation
fn compare_positions(a: Option<i32>, b: Option<i32>, deviation: u8) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => (x - y).abs() <= deviation as i32,
        (None, None) => true,
        _ => false,
    }
}

// Implement ordered comparison for ReadEntry
fn compare_reads(a: &ReadEntry, b: &ReadEntry) -> Ordering {
    // Compare by average quality score
    let quality_cmp = a.avg_quality.partial_cmp(&b.avg_quality).unwrap_or(Ordering::Equal);
    // If equal, compare by query name (in reverse order)
    if quality_cmp == Ordering::Equal {
        b.query_name.cmp(&a.query_name)
    } else {
        quality_cmp
    }
}

// Parse the integer value or return None if the value is "NA"
fn parse_int_or_na(s: &str) -> Option<i32> {
    if s == "NA" {
        None
    } else {
        s.parse().ok()
    }
}

// Convert the ASCII quality score to a quality score (optimized for speed)
fn ascii_to_quality_score(ascii_score: &str) -> f64 {
    if ascii_score == "NA" {
        return 0.0;
    }
    let bytes = ascii_score.as_bytes();
    let sum: u32 = bytes.iter().map(|&b| (b - 33) as u32).sum();
    sum as f64 / bytes.len() as f64
}

// Calculate the average quality score of the forward and reverse reads
fn average_quality_score(quality_fwd: &str, quality_rev: &str) -> f64 {
    let fwd_score = ascii_to_quality_score(quality_fwd);
    let rev_score = ascii_to_quality_score(quality_rev);
    (fwd_score + rev_score) / 2.0
}

// ------------------------------------------------------------------------------------------------
// EXTRACTION FUNCTIONS
// ------------------------------------------------------------------------------------------------

/// Optimized group building using sorted sliding window approach
/// Takes in a vector of ReadEntry objects sharing a genome_id assignment,
/// sorted by start coordinate, then iterates over the vector in order,
/// checking for position matches with previous reads whose start coordinate
/// is within `deviation` of the current read's start coordinate.
/// If a match is found, the current read is assigned to the same group as the previous read.
/// If no match is found, a new group is created.
/// Finally, all overlapping groups (those for which a single read is assigned to both groups)
/// are merged.
fn build_groups_from_sorted_reads(
    mut reads: Vec<ReadEntry>,
    deviation: u8
) -> Vec<Vec<ReadEntry>> {
    if reads.is_empty() {
        return Vec::new();
    }
    // Sort reads by coordinates for sliding window optimization
    reads.sort_by(compare_read_coordinates);
    // Track group assignment for each read (parallel arrays)
    let mut group_assignments: Vec<usize> = vec![0; reads.len()];
    let mut next_group_id = 0;
    // Track which groups need to be merged: representative_group -> set of all groups to merge
    let mut group_merges: HashMap<usize, HashSet<usize>> = HashMap::new();
    // Process reads in sorted order using sliding window
    for i in 0..reads.len() {
        let current_read = &reads[i];
        let mut matching_groups: HashSet<usize> = HashSet::new();
        // Sliding window: look backwards until more matches are impossible
        for j in (0..i).rev() {
            let prev_read = &reads[j];
            // If both reads have Some coordinates, break if the difference is greater than `deviation`
            if let (Some(curr_start), Some(prev_start)) = (current_read.aln_start, prev_read.aln_start) {
                if curr_start - prev_start > deviation as i32 {
                    break;
                }
            }
            // If current_read coordinate is None, break if previous read has Some coordinate
            if current_read.aln_start.is_none() && prev_read.aln_start.is_some() {
                break;
            }
            // Otherwise, compare fully and add to matching_groups if they match
            if match_reads(current_read, prev_read, deviation) {
                matching_groups.insert(group_assignments[j]);
            }
        }
        // Assign group based on matches found
        if matching_groups.is_empty() {
            // No matches: create new group
            group_assignments[i] = next_group_id;
            next_group_id += 1;
        } else if matching_groups.len() == 1 {
            // Single match: assign to that group
            group_assignments[i] = *matching_groups.iter().next().unwrap();
        } else {
            // Multiple matches: assign to max group and record merge for later
            let max_group = *matching_groups.iter().max().unwrap();
            group_assignments[i] = max_group;
            // Record that all matching groups should be merged with max_group
            group_merges.entry(max_group)
                .or_insert_with(|| {
                    let mut set = HashSet::new();
                    set.insert(max_group);
                    set
                })
                .extend(matching_groups);
        }
    }
    // Resolve all merges to create final group mapping
    let final_group_mapping = resolve_group_merges(group_merges);
    // Replace each group ID with its final representative group ID (resolving transitive merges)
    let final_group_assignments = group_assignments.iter()
        .map(|&group_id| *final_group_mapping.get(&group_id).unwrap_or(&group_id))
        .collect::<Vec<_>>();
    // Convert to Vec<Vec<ReadEntry>> output format
    let mut final_groups: HashMap<usize, Vec<ReadEntry>> = HashMap::new();
    for (read, &group_id) in reads.into_iter().zip(final_group_assignments.iter()) {
        final_groups.entry(group_id).or_insert_with(Vec::new).push(read);
    }
    // Return groups as Vec<Vec<ReadEntry>>
    final_groups.into_values().collect()
}

// Resolve group merges by processing in descending order of group IDs
// Assigns each group ID to the largest group ID in its merge set
fn resolve_group_merges(group_merges: HashMap<usize, HashSet<usize>>) -> HashMap<usize, usize> {
    let mut final_mapping: HashMap<usize, usize> = HashMap::new();
    // Get all group IDs that appear as keys and sort in descending order (largest first)
    let mut group_ids: Vec<usize> = group_merges.keys().copied().collect();
    group_ids.sort_by(|a, b| b.cmp(a)); // Descending order
    // Process each group ID in descending order
    for &id in &group_ids {
        // If this group ID has already been mapped to a larger group ID, use that as representative
        // Otherwise, use the group ID itself as its own representative
        let final_representative = *final_mapping.get(&id).unwrap_or(&id);
        // Map every group ID in this ID's merge set to the representative
        if let Some(groups_to_merge) = group_merges.get(&id) {
            for &group_id in groups_to_merge {
                final_mapping.insert(group_id, final_representative);
            }
        }
    }
    final_mapping
}

fn process_header_line(line: &str) -> Result<(Vec<&str>, HashMap<&str, usize>, usize), Box<dyn Error>> {
    // Split the line by tabs and collect the headers
    let headers: Vec<&str> = line.split('\t').collect();
    let header_count: usize = headers.len();
    // Build a map from header fields to indices
    let header_indices: HashMap<_, _> = headers.iter().enumerate().map(|(i, &s)| (s, i)).collect();
    // Define required header fields
    let required_headers = vec![
        "seq_id", "prim_align_genome_id_all", "prim_align_ref_start", "prim_align_ref_start_rev",
        "query_qual", "query_qual_rev"
    ];
    // Build a lookup for required headers
    let mut indices = HashMap::new();
    for header in required_headers {
        let idx = header_indices.get(header)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Missing required header: {}", header)))?;
        indices.insert(header, *idx);
    }
    // Return output
    Ok((headers, indices, header_count))
}

// Efficient function that creates ReadEntry with minimal memory allocation
fn make_read_entry(fields: &[String], indices: &HashMap<&str, usize>) -> ReadEntry {
    // Extract required fields using references to avoid cloning unnecessarily
    let query_name = fields[indices["seq_id"]].clone();
    let genome_id = &fields[indices["prim_align_genome_id_all"]];
    let ref_start_fwd = parse_int_or_na(&fields[indices["prim_align_ref_start"]]);
    let ref_start_rev = parse_int_or_na(&fields[indices["prim_align_ref_start_rev"]]);
    let quality_fwd = &fields[indices["query_qual"]];
    let quality_rev = &fields[indices["query_qual_rev"]];
    // Handle split assignments
    let genome_id_sorted: String;
    let aln_start: Option<i32>;
    let aln_end: Option<i32>;
    if genome_id.contains('/') {
        // Split genome_id by "/", sort the parts, and join them
        let parts: Vec<&str> = genome_id.split('/').collect();
        let mut sorted_parts = parts.clone();
        sorted_parts.sort();
        genome_id_sorted = sorted_parts.join("/");
        // Get the index of the first genome ID in the sorted list
        let genome_id_index = sorted_parts.iter().position(|&s| s == parts[0]).unwrap();
        // Arrange start coordinates to correspond to sorted genome IDs
        // Note: this doesn't need to handle the case where one value is None
        // because then you could never get multiple genome_ids
        (aln_start, aln_end) = if genome_id_index == 0 {
            (ref_start_fwd, ref_start_rev)
        } else {
            (ref_start_rev, ref_start_fwd)
        };
    } else {
        // If only one genome ID, use it directly
        genome_id_sorted = genome_id.to_string();
        // Normalize coordinates: if values are present, use the minimum and maximum
        // Handle cases where one value is None
        (aln_start, aln_end) = match (ref_start_fwd, ref_start_rev) {
            (Some(fwd), Some(rev)) => (Some(fwd.min(rev)), Some(fwd.max(rev))),
            (Some(fwd), None) => (Some(fwd), None),
            (None, Some(rev)) => (Some(rev), None),
            (None, None) => (None, None),
        };
    };
    let avg_quality = average_quality_score(quality_fwd, quality_rev);
    // Return the ReadEntry with minimal memory footprint
    ReadEntry { 
        query_name, 
        genome_id: genome_id_sorted, 
        aln_start, 
        aln_end, 
        avg_quality 
    }
}

// Process a chunk of lines in parallel to create ReadEntry objects
fn process_chunk_parallel(
    lines: &[String], 
    indices: &HashMap<&str, usize>,
    header_count: usize
) -> Result<Vec<ReadEntry>, Box<dyn Error>> {
    // Parse lines in parallel using rayon
    let read_entries: Result<Vec<ReadEntry>, String> = lines
        .par_iter()  // Parallel iterator from rayon
        .map(|line| {
            // Split line into fields
            let fields: Vec<String> = line.split('\t').map(|s| s.to_string()).collect();
            // Validate field count
            if fields.len() != header_count {
                return Err(format!("Invalid field count: {} (expected {})", fields.len(), header_count));
            }
            // Create ReadEntry from fields
            Ok(make_read_entry(&fields, indices))
        })
        .collect();
    // Convert String errors to Box<dyn Error>
    read_entries.map_err(|e| -> Box<dyn Error> { 
        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into() 
    })
}

fn extract_read_groups(input_path: &str,
    chunk_size: u32,
    deviation: u8
) -> Result<(String, HashMap<String, Vec<Vec<ReadEntry>>>, usize), Box<dyn Error>> {
    // Open the input file
    let reader = open_reader(input_path)?;
    // Process the header line and derive the required fields
    let mut lines = reader.lines();
    let header_line = lines.next().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Empty input file"))??;
    let (headers, indices, header_count) = process_header_line(&header_line)?;
    // Create the output header line
    let mut headers_out = headers.clone();
    headers_out.push("prim_align_dup_exemplar");
    let header_out = headers_out.join("\t");
    // Get the seq_id column index for later use
    let seq_id_index = indices["seq_id"];
    // Collect reads by genome_id
    let mut genome_accumulators: HashMap<String, Vec<ReadEntry>> = HashMap::new();
    // Read and process the input file in chunks
    let mut line_buffer = Vec::new();
    for line in lines {
        let line = line?;
        line_buffer.push(line);
        // Process chunk when buffer is full
        if line_buffer.len() >= chunk_size as usize {
            // Process this chunk in parallel
            let read_entries = process_chunk_parallel(&line_buffer, &indices, header_count)?;
            // Partition reads by genome_id
            for read_entry in read_entries {
                genome_accumulators.entry(read_entry.genome_id.clone())
                    .or_insert_with(Vec::new)
                    .push(read_entry);
            }
            // Clear the buffer
            line_buffer.clear();
        }
    }
    // Process remaining lines in the buffer
    if !line_buffer.is_empty() {
        let read_entries = process_chunk_parallel(&line_buffer, &indices, header_count)?;
        for read_entry in read_entries {
            genome_accumulators.entry(read_entry.genome_id.clone())
                .or_insert_with(Vec::new)
                .push(read_entry);
        }
    }
    // Process reads for each genome_id into read groups using optimized sorting approach
    let genome_results: Vec<(String, Vec<Vec<ReadEntry>>)> = genome_accumulators
        .into_par_iter()
        .map(|(genome_id, reads)| {
            // Use optimized sorted sliding window approach
            let groups = build_groups_from_sorted_reads(reads, deviation);
            (genome_id, groups)
        })
        .collect();
    // Collect results back into the main groups HashMap
    let mut final_groups = HashMap::new();
    for (genome_id, genome_group_list) in genome_results {
        final_groups.insert(genome_id, genome_group_list);
    }
    Ok((header_out, final_groups, seq_id_index))
}

// ------------------------------------------------------------------------------------------------
// PROCESSING FUNCTIONS
// ------------------------------------------------------------------------------------------------

// Process duplicate groups to create exemplar mapping and metadata (focused on group processing)
fn process_read_groups(
    groups: HashMap<String, Vec<Vec<ReadEntry>>>,
    deviation: u8
) -> Result<(ExemplarMap, Vec<DuplicateGroup>), Box<dyn Error>> {
    // Flatten all duplicate groups with their genome_id for parallel processing
    let all_groups: Vec<(String, Vec<ReadEntry>)> = groups
        .into_iter()
        .flat_map(|(genome_id, id_groups)| {
            id_groups.into_iter().map(move |dup_group| (genome_id.clone(), dup_group))
        })
        .collect();
    // Process all groups in parallel
    let group_results: Vec<(DuplicateGroup, Vec<(String, String, String)>)> = all_groups
        .par_iter()  // Parallel iterator
        .map(|(genome_id, dup_group)| {
            // Find the exemplar using compare_reads
            let exemplar = dup_group.iter().max_by(|a, b| compare_reads(a, b)).unwrap();
            let exemplar_name = exemplar.query_name.clone();
            // Calculate size of duplicate group
            let dup_count = dup_group.len();
            // Calculate fraction of pairwise matches (as a QC metric for the group as a whole)
            let pairwise_match_frac: f64;
            if dup_count == 1 {
                pairwise_match_frac = 1.0;
            } else {
                // Stage 2 Multithreading: Parallel pairwise matching
                // Generate all pairs (i,j) where i < j and process them in parallel
                let dup_count_float: f64 = dup_count as f64;
                let n_pairs: f64 = dup_count_float * (dup_count_float - 1.0) / 2.0;
                // Use rayon to parallelize pairwise comparisons
                let pairwise_match_count: f64 = (0..dup_count)
                    .into_par_iter()  // Parallel iterator
                    .flat_map(|i| (i + 1..dup_count).into_par_iter().map(move |j| (i, j)))
                    .map(|(i, j)| {
                        let read_i = &dup_group[i];
                        let read_j = &dup_group[j];
                        if match_reads(read_i, read_j, deviation) { 1.0 } else { 0.0 }
                    })
                    .sum();  // Rayon's parallel sum reduction
                pairwise_match_frac = pairwise_match_count / n_pairs;
            }
            // Create duplicate group metadata
            let dup_group_info = DuplicateGroup {
                genome_id: genome_id.clone(),
                exemplar_name: exemplar_name.clone(),
                group_size: dup_count,
                pairwise_match_frac,
            };
            // Create exemplar mappings for this group
            let exemplar_mappings: Vec<(String, String, String)> = dup_group
                .iter()
                .map(|read_entry| {
                    (read_entry.query_name.clone(), genome_id.clone(), exemplar_name.clone())
                })
                .collect();
            
            (dup_group_info, exemplar_mappings)
        })
        .collect();
    
    // Collect results into final data structures
    let mut exemplar_map = ExemplarMap::new();
    let mut duplicate_groups = Vec::new();
    for (dup_group_info, exemplar_mappings) in group_results {
        duplicate_groups.push(dup_group_info);
        for (query_name, genome_id, exemplar_name) in exemplar_mappings {
            exemplar_map.insert(query_name, (genome_id, exemplar_name));
        }
    }
    Ok((exemplar_map, duplicate_groups))
}

// ------------------------------------------------------------------------------------------------
// WRITING FUNCTIONS
// ------------------------------------------------------------------------------------------------

// Write duplicate group metadata file (no file streaming required)
fn write_metadata_file(
    duplicate_groups: &Vec<DuplicateGroup>,
    output_path_meta: &str,
) -> Result<(), Box<dyn Error>> {
    // Open the metadata output file
    let mut writer_meta = open_writer(output_path_meta)?;
    // Write header
    let header_meta = "prim_align_genome_id_all\tprim_align_dup_exemplar\tprim_align_dup_count\tprim_align_dup_pairwise_match_frac";
    writeln!(writer_meta, "{}", header_meta)?;
    // Write duplicate group metadata (once per group)
    for dup_group in duplicate_groups {
        writeln!(writer_meta, "{}\t{}\t{}\t{}", 
                dup_group.genome_id, dup_group.exemplar_name, dup_group.group_size, dup_group.pairwise_match_frac)?;
    }
    Ok(())
}

// Stream through file and add exemplar information
fn write_database_file(
    input_path: &str,
    header_out: &str,
    exemplar_map: &ExemplarMap,
    seq_id_index: usize,
    output_path_db: &str,
) -> Result<(), Box<dyn Error>> {
    // Open input file for second pass
    let reader = open_reader(input_path)?;
    // Open the database output file
    let mut writer_db = open_writer(output_path_db)?;
    // Write header
    writeln!(writer_db, "{}", header_out)?;
    // Process input file line by line for output generation
    let mut lines = reader.lines();
    let _header_line = lines.next(); // Skip header
    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split('\t').collect();
        let query_name = fields[seq_id_index];
        // Look up exemplar for this read
        if let Some((_genome_id, exemplar_name)) = exemplar_map.get(query_name) {
            writeln!(writer_db, "{}\t{}", line, exemplar_name)?;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Could not find exemplar for read: {}", query_name)
            ).into());
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// TOP-LEVEL FUNCTIONS
// ------------------------------------------------------------------------------------------------

// Two-pass processing for improved memory efficiency
fn process_tsv(input_path: &str,
    output_path_db: &str,
    output_path_meta: &str,
    chunk_size: u32,
    deviation: u8) -> Result<(), Box<dyn Error>> {
    // Extract read groups from the input file
    let (header_out, groups, seq_id_index) = extract_read_groups(input_path, chunk_size, deviation)?;
    // Process duplicate groups to create exemplar mapping and metadata
    let (exemplar_map, duplicate_groups) = process_read_groups(groups, deviation)?;
    // Write metadata file
    write_metadata_file(&duplicate_groups, output_path_meta)?;
    // Write database file
    write_database_file(input_path, &header_out, &exemplar_map, seq_id_index, output_path_db)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args = Args::parse();
    // Configure rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.num_threads as usize)
        .build_global()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
            format!("Failed to configure thread pool: {}", e)))?;
    // Run the main processing function
    return process_tsv(&args.input, &args.output_db, &args.output_meta, args.chunk_size, args.deviation);
}

// ------------------------------------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Header used by the make_read_entry tests, matching the columns the tool requires
    const HEADERS: [&str; 7] = [
        "seq_id",
        "prim_align_genome_id_all",
        "prim_align_ref_start",
        "prim_align_ref_start_rev",
        "query_qual",
        "query_qual_rev",
        // Present in the pipeline's hits table but not read by this version, so the
        // fixtures here stay valid once #969 starts keying on it.
        "prim_align_fragment_length",
    ];

    // Build the (fields, indices) pair that make_read_entry expects from a single row
    fn row(values: &[&str]) -> (Vec<String>, HashMap<&'static str, usize>) {
        assert_eq!(values.len(), HEADERS.len());
        let fields = values.iter().map(|v| v.to_string()).collect();
        let indices = HEADERS.iter().enumerate().map(|(i, &h)| (h, i)).collect();
        (fields, indices)
    }

    // Construct a ReadEntry directly, bypassing parsing. Grouping and matching tests use this
    // so they exercise the algorithm rather than the column layout.
    fn entry(
        name: &str,
        genome: &str,
        start: Option<i32>,
        end: Option<i32>,
        quality: f64,
    ) -> ReadEntry {
        ReadEntry {
            query_name: name.to_string(),
            genome_id: genome.to_string(),
            aln_start: start,
            aln_end: end,
            avg_quality: quality,
        }
    }

    // Groups come back in HashMap order, and reads within a group in sort order, so normalise
    // both before comparing
    fn group_names(groups: Vec<Vec<ReadEntry>>) -> Vec<Vec<String>> {
        let mut out: Vec<Vec<String>> = groups
            .into_iter()
            .map(|g| {
                let mut names: Vec<String> = g.into_iter().map(|r| r.query_name).collect();
                names.sort();
                names
            })
            .collect();
        out.sort();
        out
    }

    // --- Field parsing ---

    #[test]
    fn parse_int_or_na_reads_integers_and_na() {
        assert_eq!(parse_int_or_na("0"), Some(0));
        assert_eq!(parse_int_or_na("1234"), Some(1234));
        assert_eq!(parse_int_or_na("-5"), Some(-5));
        assert_eq!(parse_int_or_na("NA"), None);
    }

    #[test]
    fn ascii_to_quality_score_averages_phred_offsets() {
        // '!' is Phred 0, '+' is Phred 10, 'I' is Phred 40
        assert_eq!(ascii_to_quality_score("!"), 0.0);
        assert_eq!(ascii_to_quality_score("+"), 10.0);
        assert_eq!(ascii_to_quality_score("III"), 40.0);
        assert_eq!(ascii_to_quality_score("!I"), 20.0);
        // A missing quality string scores zero rather than erroring
        assert_eq!(ascii_to_quality_score("NA"), 0.0);
    }

    #[test]
    fn average_quality_score_means_the_two_mates() {
        // 40 and 0 average to 20
        assert_eq!(average_quality_score("III", "!!!"), 20.0);
        // An absent mate contributes zero, halving the pair's score
        assert_eq!(average_quality_score("III", "NA"), 20.0);
    }

    // --- Position comparison ---

    #[test]
    fn compare_positions_respects_the_deviation_tolerance() {
        // Exact match at zero tolerance
        assert!(compare_positions(Some(100), Some(100), 0));
        assert!(!compare_positions(Some(100), Some(101), 0));
        // The boundary itself matches; one past it does not
        assert!(compare_positions(Some(100), Some(101), 1));
        assert!(!compare_positions(Some(100), Some(102), 1));
        assert!(compare_positions(Some(100), Some(102), 2));
        assert!(!compare_positions(Some(100), Some(103), 2));
        // Tolerance is symmetric
        assert!(compare_positions(Some(102), Some(100), 2));
    }

    #[test]
    fn compare_positions_never_matches_a_known_against_an_unknown() {
        assert!(!compare_positions(Some(100), None, 2));
        assert!(!compare_positions(None, Some(100), 2));
    }

    #[test]
    fn order_positions_sorts_unknowns_last() {
        assert_eq!(order_positions(Some(1), Some(2)), Ordering::Less);
        assert_eq!(order_positions(Some(2), Some(1)), Ordering::Greater);
        assert_eq!(order_positions(Some(1), Some(1)), Ordering::Equal);
        assert_eq!(order_positions(Some(1), None), Ordering::Less);
        assert_eq!(order_positions(None, Some(1)), Ordering::Greater);
        assert_eq!(order_positions(None, None), Ordering::Equal);
    }

    #[test]
    fn compare_read_coordinates_orders_by_start_then_end() {
        let a = entry("a", "g", Some(10), Some(200), 30.0);
        let b = entry("b", "g", Some(20), Some(100), 30.0);
        let c = entry("c", "g", Some(10), Some(300), 30.0);
        // Start dominates, even when the end runs the other way
        assert_eq!(compare_read_coordinates(&a, &b), Ordering::Less);
        // Equal starts fall through to the end coordinate
        assert_eq!(compare_read_coordinates(&a, &c), Ordering::Less);
        assert_eq!(compare_read_coordinates(&a, &a), Ordering::Equal);
    }

    // --- Read entry construction ---

    #[test]
    fn make_read_entry_keys_a_pair_on_its_two_mate_starts() {
        // A pair uses the two mate start coordinates, not the fragment's right edge, which
        // for these 100 bp mates is 900.
        // TODO: fix this bug, reported in #948.
        for (fwd, rev) in [("500", "800"), ("800", "500")] {
            let (fields, indices) = row(&["r1", "genome_a", fwd, rev, "IIII", "IIII", "400"]);
            let e = make_read_entry(&fields, &indices);
            assert_eq!((e.aln_start, e.aln_end), (Some(500), Some(800)));
        }
    }

    #[test]
    fn make_read_entry_keys_an_incomplete_pair_on_one_coordinate() {
        // With one mate unaligned, reads are compared using only the start.
        for (fwd, rev) in [("500", "NA"), ("NA", "500")] {
            let (fields, indices) = row(&["r1", "genome_a", fwd, rev, "IIII", "IIII", "NA"]);
            let e = make_read_entry(&fields, &indices);
            assert_eq!((e.aln_start, e.aln_end), (Some(500), None));
        }
    }

    #[test]
    fn make_read_entry_keys_an_unaligned_pair_on_nothing() {
        let (fields, indices) = row(&["r1", "genome_a", "NA", "NA", "IIII", "IIII", "NA"]);
        let e = make_read_entry(&fields, &indices);
        assert_eq!((e.aln_start, e.aln_end), (None, None));
    }

    #[test]
    fn make_read_entry_merges_short_fragments_that_share_a_start() {
        // A fragment shorter than the read is covered end to end by both mates, so both
        // report the same leftmost coordinate and the key collapses to (start, start).
        // TODO: fix this bug, reported in #948.
        let (fields, indices) = row(&["r1", "genome_a", "400", "400", "IIII", "IIII", "40"]);
        let short = make_read_entry(&fields, &indices);
        let (fields, indices) = row(&["r2", "genome_a", "400", "400", "IIII", "IIII", "80"]);
        let long = make_read_entry(&fields, &indices);
        assert_eq!((short.aln_start, short.aln_end), (Some(400), Some(400)));
        assert_eq!((long.aln_start, long.aln_end), (Some(400), Some(400)));
        assert!(match_reads(&short, &long, 2));
    }

    #[test]
    fn make_read_entry_accepts_a_complete_pair_with_no_fragment_length() {
        // Both mates aligned to one genome but no fragment length, which is not valid.
        // TODO: guard against this.
        let (fields, indices) = row(&["r1", "genome_a", "500", "800", "IIII", "IIII", "NA"]);
        let e = make_read_entry(&fields, &indices);
        assert_eq!((e.aln_start, e.aln_end), (Some(500), Some(800)));
    }

    #[test]
    fn make_read_entry_carries_name_genome_and_quality() {
        let (fields, indices) = row(&["r1", "genome_a", "500", "800", "III", "!!!", "300"]);
        let e = make_read_entry(&fields, &indices);
        assert_eq!(e.query_name, "r1");
        assert_eq!(e.genome_id, "genome_a");
        assert_eq!(e.avg_quality, 20.0);
    }

    #[test]
    fn make_read_entry_sorts_split_genome_ids_and_their_coordinates_together() {
        // Mates on two genomes: the ID is sorted, and the coordinates are permuted to match,
        // so that the same pair of genomes always produces the same key regardless of which
        // mate landed on which
        let (fields, indices) = row(&["r1", "genome_b/genome_a", "500", "800", "IIII", "IIII", "NA"]);
        let e = make_read_entry(&fields, &indices);
        assert_eq!(e.genome_id, "genome_a/genome_b");
        // genome_a is the reverse mate's genome here, so its coordinate leads
        assert_eq!(e.aln_start, Some(800));
        assert_eq!(e.aln_end, Some(500));

        // The same pair the other way round yields an identical key
        let (fields, indices) = row(&["r2", "genome_a/genome_b", "800", "500", "IIII", "IIII", "NA"]);
        let e = make_read_entry(&fields, &indices);
        assert_eq!(e.genome_id, "genome_a/genome_b");
        assert_eq!(e.aln_start, Some(800));
        assert_eq!(e.aln_end, Some(500));
    }

    // --- Header handling ---

    #[test]
    fn process_header_line_indexes_every_required_column() {
        let header = HEADERS.join("\t");
        let (headers, indices, count) = process_header_line(&header).unwrap();
        assert_eq!(count, HEADERS.len());
        assert_eq!(headers, HEADERS.to_vec());
        // prim_align_fragment_length is in the fixture but not required by this version.
        for required in HEADERS.iter().filter(|&&h| h != "prim_align_fragment_length") {
            assert!(indices.contains_key(required));
        }
    }

    #[test]
    fn process_header_line_tolerates_extra_columns() {
        let header = format!("{}\textra_column", HEADERS.join("\t"));
        let (_headers, indices, count) = process_header_line(&header).unwrap();
        assert_eq!(count, HEADERS.len() + 1);
        assert_eq!(indices["seq_id"], 0);
    }

    #[test]
    fn process_header_line_rejects_a_missing_required_column() {
        let header = HEADERS
            .iter()
            .filter(|&&h| h != "query_qual_rev")
            .copied()
            .collect::<Vec<_>>()
            .join("\t");
        let err = process_header_line(&header).unwrap_err().to_string();
        assert!(
            err.contains("Missing required header"),
            "unexpected error: {err}"
        );
        assert!(err.contains("query_qual_rev"), "unexpected error: {err}");
    }

    // --- Matching ---

    #[test]
    fn match_reads_requires_the_same_genome() {
        let a = entry("a", "genome_a", Some(100), Some(300), 30.0);
        let b = entry("b", "genome_b", Some(100), Some(300), 30.0);
        assert!(!match_reads(&a, &b, 2));
    }

    #[test]
    fn match_reads_treats_absent_coordinates_as_agreement() {
        // Two reads with one mate match on their single shared coordinate. Two reads with
        // no coordinates at all match on nothing.
        // TODO: fix this bug, reported in #948.
        let a = entry("a", "g", Some(100), None, 30.0);
        let b = entry("b", "g", Some(100), None, 30.0);
        assert!(match_reads(&a, &b, 0));
        let c = entry("c", "g", None, None, 30.0);
        let d = entry("d", "g", None, None, 30.0);
        assert!(match_reads(&c, &d, 0));
    }

    #[test]
    fn match_reads_requires_both_coordinates_to_agree() {
        let a = entry("a", "g", Some(100), Some(300), 30.0);
        // Both within tolerance
        assert!(match_reads(
            &a,
            &entry("b", "g", Some(101), Some(301), 30.0),
            1
        ));
        // Start agrees, end does not
        assert!(!match_reads(
            &a,
            &entry("c", "g", Some(101), Some(310), 30.0),
            1
        ));
        // End agrees, start does not
        assert!(!match_reads(
            &a,
            &entry("d", "g", Some(110), Some(301), 30.0),
            1
        ));
    }

    // --- Exemplar selection ---

    #[test]
    fn compare_reads_ranks_by_quality_then_breaks_ties_on_name() {
        let high = entry("zzz", "g", Some(1), Some(2), 36.0);
        let low = entry("aaa", "g", Some(1), Some(2), 30.0);
        // Quality dominates, regardless of name
        assert_eq!(compare_reads(&high, &low), Ordering::Greater);
        // Equal quality falls back to the lexicographically smaller name winning
        let a = entry("aaa", "g", Some(1), Some(2), 30.0);
        let b = entry("bbb", "g", Some(1), Some(2), 30.0);
        assert_eq!(compare_reads(&a, &b), Ordering::Greater);
        // max_by therefore selects the smallest name among equals
        let group = vec![b.clone(), a.clone()];
        let exemplar = group.iter().max_by(|x, y| compare_reads(x, y)).unwrap();
        assert_eq!(exemplar.query_name, "aaa");
    }

    // --- Grouping ---

    #[test]
    fn build_groups_from_sorted_reads_handles_an_empty_input() {
        assert!(build_groups_from_sorted_reads(Vec::new(), 1).is_empty());
    }

    #[test]
    fn build_groups_from_sorted_reads_separates_reads_beyond_the_tolerance() {
        let reads = vec![
            entry("a", "g", Some(100), Some(300), 30.0),
            entry("b", "g", Some(101), Some(301), 30.0),
            entry("c", "g", Some(200), Some(400), 30.0),
        ];
        // At tolerance 1, a and b group and c stands alone
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads.clone(), 1)),
            vec![vec!["a", "b"], vec!["c"]]
        );
        // At tolerance 0, all three are distinct
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 0)),
            vec![vec!["a"], vec!["b"], vec!["c"]]
        );
    }

    #[test]
    fn build_groups_from_sorted_reads_is_independent_of_input_order() {
        let reads = vec![
            entry("c", "g", Some(200), Some(400), 30.0),
            entry("a", "g", Some(100), Some(300), 30.0),
            entry("b", "g", Some(101), Some(301), 30.0),
        ];
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 1)),
            vec![vec!["a", "b"], vec!["c"]]
        );
    }

    #[test]
    fn build_groups_from_sorted_reads_merges_chains_transitively() {
        // a-b and b-c each match at tolerance 1, but a-c differ by 2. Matching is
        // intransitive, and the algorithm resolves that by merging the whole chain.
        let reads = vec![
            entry("a", "g", Some(100), Some(300), 30.0),
            entry("b", "g", Some(101), Some(301), 30.0),
            entry("c", "g", Some(102), Some(302), 30.0),
        ];
        assert!(!match_reads(&reads[0], &reads[2], 1));
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 1)),
            vec![vec!["a", "b", "c"]]
        );
    }

    #[test]
    fn build_groups_from_sorted_reads_keeps_different_genomes_apart() {
        // Identical coordinates on different genomes are never duplicates, even though the
        // sliding window will compare them
        let reads = vec![
            entry("a", "genome_a", Some(100), Some(300), 30.0),
            entry("b", "genome_b", Some(100), Some(300), 30.0),
        ];
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 2)),
            vec![vec!["a"], vec!["b"]]
        );
    }

    #[test]
    fn build_groups_from_sorted_reads_splits_on_the_end_coordinate_alone() {
        // Sharing a start is not enough: the sliding window still compares both coordinates
        let reads = vec![
            entry("a", "g", Some(100), Some(300), 30.0),
            entry("b", "g", Some(100), Some(900), 30.0),
        ];
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 1)),
            vec![vec!["a"], vec!["b"]]
        );
    }

    // --- Merge resolution ---

    #[test]
    fn resolve_group_merges_maps_every_member_to_the_largest_id() {
        let mut merges = HashMap::new();
        merges.insert(5, HashSet::from([1, 3, 5]));
        let mapping = resolve_group_merges(merges);
        assert_eq!(mapping[&1], 5);
        assert_eq!(mapping[&3], 5);
        assert_eq!(mapping[&5], 5);
    }

    #[test]
    fn resolve_group_merges_follows_chained_merges_to_one_representative() {
        // Group 7 absorbs 4, and 4 had already absorbed 2: all three land on 7
        let mut merges = HashMap::new();
        merges.insert(4, HashSet::from([2, 4]));
        merges.insert(7, HashSet::from([4, 7]));
        let mapping = resolve_group_merges(merges);
        assert_eq!(mapping[&7], 7);
        assert_eq!(mapping[&4], 7);
        assert_eq!(mapping[&2], 7);
    }
}
