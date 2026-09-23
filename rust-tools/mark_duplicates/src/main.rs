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
    key: DupKey,
    avg_quality: f64,
    // A lone mate attached to a complete pair's group. Attached reads are duplicates of
    // the group but are not compared against it, and never become its exemplar.
    attached: bool,
}

// One mate's unclipped 5' reference position, and the strand it aligned to.
//
// The 5' end is the unclipped start for a forward mate and the unclipped end for a
// reverse one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MateEnd {
    five_prime: i32,
    reverse: bool,
}

// The coordinate key a read is matched on. Every coordinate it holds is a mate's
// unclipped 5' position, named `_5p`. Reads carrying different variants are not
// comparable and never match. A lone mate reaches a pair's group through
// `attach_lone_mates` instead, as `samtools markdup` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DupKey {
    // Both mates aligned to one genome, on opposite strands (FR or RF).
    // The two coordinates are in strand order to distinguish FR from RF.
    PairOppositeStrands { forward_5p: i32, reverse_5p: i32 },
    // Both mates aligned to one genome, on the same strand (FF or RR).
    // The two coordinates are sorted.
    PairSameStrand { left_5p: i32, right_5p: i32, reverse: bool },
    // Mates aligned to two genomes: keyed per mate, in the order of the sorted genome
    // pair that `genome_id` carries.
    SplitGenomes { first_mate: MateEnd, second_mate: MateEnd },
    // One mate aligned.
    OneMateAligned(MateEnd),
    // Neither mate aligned, so there is no coordinate to compare and nothing matches.
    // `samtools markdup` does not key an unmapped read at all.
    NeitherAligned,
}

impl DupKey {
    // Leading coordinate.
    fn sort_start(&self) -> Option<i32> {
        match *self {
            DupKey::PairOppositeStrands { forward_5p, reverse_5p } => {
                Some(forward_5p.min(reverse_5p))
            }
            DupKey::PairSameStrand { left_5p, .. } => Some(left_5p),
            DupKey::SplitGenomes { first_mate, second_mate } => {
                Some(first_mate.five_prime.min(second_mate.five_prime))
            }
            DupKey::OneMateAligned(mate) => Some(mate.five_prime),
            DupKey::NeitherAligned => None,
        }
    }

    // Trailing coordinate.
    fn sort_end(&self) -> Option<i32> {
        match *self {
            DupKey::PairOppositeStrands { forward_5p, reverse_5p } => {
                Some(forward_5p.max(reverse_5p))
            }
            DupKey::PairSameStrand { right_5p, .. } => Some(right_5p),
            DupKey::SplitGenomes { first_mate, second_mate } => {
                Some(first_mate.five_prime.max(second_mate.five_prime))
            }
            DupKey::OneMateAligned(_) | DupKey::NeitherAligned => None,
        }
    }

    // The two mates of a complete pair, or nothing for a lone mate or an unaligned read.
    fn pair_mates(&self) -> Option<[MateEnd; 2]> {
        match *self {
            DupKey::PairOppositeStrands { forward_5p, reverse_5p } => Some([
                MateEnd { five_prime: forward_5p, reverse: false },
                MateEnd { five_prime: reverse_5p, reverse: true },
            ]),
            DupKey::PairSameStrand { left_5p, right_5p, reverse } => Some([
                MateEnd { five_prime: left_5p, reverse },
                MateEnd { five_prime: right_5p, reverse },
            ]),
            DupKey::SplitGenomes { first_mate, second_mate } => Some([first_mate, second_mate]),
            DupKey::OneMateAligned(_) | DupKey::NeitherAligned => None,
        }
    }

    // Whether two keys place their reads at the same position, within the tolerance.
    fn matches(&self, other: &DupKey, deviation: u8) -> bool {
        match (*self, *other) {
            (
                DupKey::PairOppositeStrands { forward_5p: a_fwd, reverse_5p: a_rev },
                DupKey::PairOppositeStrands { forward_5p: b_fwd, reverse_5p: b_rev },
            ) => within(a_fwd, b_fwd, deviation) && within(a_rev, b_rev, deviation),
            (
                DupKey::PairSameStrand { left_5p: a_left, right_5p: a_right, reverse: a_rev },
                DupKey::PairSameStrand { left_5p: b_left, right_5p: b_right, reverse: b_rev },
            ) => {
                a_rev == b_rev
                    && within(a_left, b_left, deviation)
                    && within(a_right, b_right, deviation)
            }
            (
                DupKey::SplitGenomes { first_mate: a_first, second_mate: a_second },
                DupKey::SplitGenomes { first_mate: b_first, second_mate: b_second },
            ) => mates_match(a_first, b_first, deviation) && mates_match(a_second, b_second, deviation),
            (DupKey::OneMateAligned(a), DupKey::OneMateAligned(b)) => mates_match(a, b, deviation),
            // Two reads with no coordinates say nothing about each other, and keys of
            // different kinds are not comparable
            _ => false,
        }
    }
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

// Sort ReadEntries by their key's leading coordinate, then its trailing one
// None values are treated as larger than any Some value (sorted to the end)
fn compare_read_coordinates(a: &ReadEntry, b: &ReadEntry) -> Ordering {
    match order_positions(a.key.sort_start(), b.key.sort_start()) {
        Ordering::Equal => order_positions(a.key.sort_end(), b.key.sort_end()),
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
    a.genome_id == b.genome_id && a.key.matches(&b.key, deviation)
}

// Whether two coordinates agree within the deviation
fn within(a: i32, b: i32, deviation: u8) -> bool {
    (a - b).abs() <= deviation as i32
}

// Whether two mates are on the same strand at the same position, within the deviation
fn mates_match(a: MateEnd, b: MateEnd, deviation: u8) -> bool {
    a.reverse == b.reverse && within(a.five_prime, b.five_prime, deviation)
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

// Parse a coordinate: an integer, or None for "NA". Anything else is bad input, and
// silently reading it as an absent coordinate would change how the read is keyed.
fn parse_coordinate(s: &str, query_name: &str, field: &str) -> Result<Option<i32>, String> {
    match parse_int_or_na(s) {
        Some(value) => Ok(Some(value)),
        None if s == "NA" => Ok(None),
        None => Err(format!(
            "Read {query_name} has an unreadable {field}: {s}"
        )),
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

// Calculate the average quality score across both mates
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
            if let (Some(curr_start), Some(prev_start)) =
                (current_read.key.sort_start(), prev_read.key.sort_start())
            {
                if curr_start - prev_start > deviation as i32 {
                    break;
                }
            }
            // If current_read coordinate is None, break if previous read has Some coordinate
            if current_read.key.sort_start().is_none() && prev_read.key.sort_start().is_some() {
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

// Attach each lone mate to the group of a complete pair sharing its 5' end and strand,
// as `samtools markdup` does. The attachment is one-way: the lone mate joins one pair's
// group, so a shared coordinate can never merge two groups of pairs.
//
// Lone mates that leave a group of lone mates can take with them the coordinates that
// chained the rest together. The reads left behind stay in the group they were built
// into rather than being regrouped, so attaching a lone mate never creates an exemplar.
fn attach_lone_mates(groups: &mut Vec<Vec<ReadEntry>>, deviation: u8) {
    // Index every complete pair's mates by strand and 5' end, recording where each sits
    let mut mates: Vec<(bool, i32, usize, usize)> = Vec::new();
    for (group_index, group) in groups.iter().enumerate() {
        for (read_index, read) in group.iter().enumerate() {
            if let Some(pair_mates) = read.key.pair_mates() {
                for mate in pair_mates {
                    mates.push((mate.reverse, mate.five_prime, group_index, read_index));
                }
            }
        }
    }
    if mates.is_empty() {
        return;
    }
    mates.sort_unstable_by_key(|&(reverse, five_prime, _, _)| (reverse, five_prime));
    // Choose every destination against the groups as built, then rebuild them in one pass
    let destinations: Vec<Vec<Option<usize>>> = groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|read| match read.key {
                    DupKey::OneMateAligned(mate) => {
                        nearest_pair_group(&mates, groups, mate, deviation)
                    }
                    _ => None,
                })
                .collect()
        })
        .collect();
    let mut rebuilt: Vec<Vec<ReadEntry>> = vec![Vec::new(); groups.len()];
    for (group_index, group) in std::mem::take(groups).into_iter().enumerate() {
        for (read_index, mut read) in group.into_iter().enumerate() {
            match destinations[group_index][read_index] {
                Some(target) => {
                    read.attached = true;
                    rebuilt[target].push(read);
                }
                None => rebuilt[group_index].push(read),
            }
        }
    }
    rebuilt.retain(|group| !group.is_empty());
    *groups = rebuilt;
}

// The group of the pair mate closest to this lone mate, breaking ties on read name so
// the choice does not depend on the order the groups were built in.
fn nearest_pair_group(
    mates: &[(bool, i32, usize, usize)],
    groups: &[Vec<ReadEntry>],
    mate: MateEnd,
    deviation: u8,
) -> Option<usize> {
    let deviation = deviation as i32;
    let first = mates.partition_point(|&(reverse, five_prime, _, _)| {
        (reverse, five_prime) < (mate.reverse, mate.five_prime - deviation)
    });
    let mut best: Option<(i32, &str, usize)> = None;
    for &(reverse, five_prime, group_index, read_index) in &mates[first..] {
        if reverse != mate.reverse || five_prime > mate.five_prime + deviation {
            break;
        }
        let name = groups[group_index][read_index].query_name.as_str();
        let distance = (five_prime - mate.five_prime).abs();
        let better = match best {
            None => true,
            Some((best_distance, best_name, _)) => (distance, name) < (best_distance, best_name),
        };
        if better {
            best = Some((distance, name, group_index));
        }
    }
    best.map(|(_, _, group_index)| group_index)
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
        "seq_id", "prim_align_genome_id_all",
        "prim_align_ref_start_unclipped", "prim_align_ref_start_unclipped_rev",
        "prim_align_ref_end_unclipped", "prim_align_ref_end_unclipped_rev",
        "prim_align_query_rc", "prim_align_query_rc_rev",
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

// Parse one mate's unclipped bounds and strand, or None if that mate did not align
fn make_mate_end(
    start: &str,
    end: &str,
    reverse: &str,
    query_name: &str,
    mate: &str,
) -> Result<Option<MateEnd>, String> {
    let start = parse_coordinate(start, query_name, &format!("{mate} unclipped start"))?;
    let end = parse_coordinate(end, query_name, &format!("{mate} unclipped end"))?;
    match (start, end) {
        // An unaligned mate has no CIGAR, and so no unclipped bounds
        (None, None) => Ok(None),
        (Some(start), Some(end)) => {
            // The strand decides which bound is the 5' end, so a missing one is an
            // error rather than a default
            let reverse = match reverse {
                "True" => true,
                "False" => false,
                other => {
                    return Err(format!(
                        "Read {query_name} has an aligned {mate} with strand {other}"
                    ))
                }
            };
            let five_prime = if reverse { end } else { start };
            Ok(Some(MateEnd { five_prime, reverse }))
        }
        _ => Err(format!(
            "Read {query_name} has only one unclipped coordinate for {mate}"
        )),
    }
}

// Efficient function that creates ReadEntry with minimal memory allocation
fn make_read_entry(
    fields: &[String],
    indices: &HashMap<&str, usize>,
) -> Result<ReadEntry, String> {
    // Extract required fields using references to avoid cloning unnecessarily
    let query_name = fields[indices["seq_id"]].clone();
    let genome_id = &fields[indices["prim_align_genome_id_all"]];
    let mate_1 = make_mate_end(
        &fields[indices["prim_align_ref_start_unclipped"]],
        &fields[indices["prim_align_ref_end_unclipped"]],
        &fields[indices["prim_align_query_rc"]],
        &query_name,
        "mate 1",
    )?;
    let mate_2 = make_mate_end(
        &fields[indices["prim_align_ref_start_unclipped_rev"]],
        &fields[indices["prim_align_ref_end_unclipped_rev"]],
        &fields[indices["prim_align_query_rc_rev"]],
        &query_name,
        "mate 2",
    )?;
    let quality_fwd = &fields[indices["query_qual"]];
    let quality_rev = &fields[indices["query_qual_rev"]];
    // Handle split assignments: sort the genome IDs so the same pair of genomes always
    // gives the same ID, and record whether that reordered the mates
    let genome_id_sorted: String;
    let mut mates_swapped = false;
    let split_genomes = genome_id.contains('/');
    if split_genomes {
        let parts: Vec<&str> = genome_id.split('/').collect();
        let mut sorted_parts = parts.clone();
        sorted_parts.sort();
        genome_id_sorted = sorted_parts.join("/");
        mates_swapped = sorted_parts.iter().position(|&s| s == parts[0]).unwrap() != 0;
    } else {
        // If only one genome ID, use it directly
        genome_id_sorted = genome_id.to_string();
    }
    let key = match (mate_1, mate_2) {
        // Mates on two genomes are keyed per mate, in sorted-genome order
        (Some(mate_1), Some(mate_2)) if split_genomes => {
            let (first_mate, second_mate) = if mates_swapped {
                (mate_2, mate_1)
            } else {
                (mate_1, mate_2)
            };
            DupKey::SplitGenomes { first_mate, second_mate }
        }
        // On one genome, opposite strands identify the mates without a coordinate sort
        (Some(mate_1), Some(mate_2)) if mate_1.reverse != mate_2.reverse => {
            let (forward, reverse) = if mate_1.reverse {
                (mate_2, mate_1)
            } else {
                (mate_1, mate_2)
            };
            DupKey::PairOppositeStrands {
                forward_5p: forward.five_prime,
                reverse_5p: reverse.five_prime,
            }
        }
        // On one strand there is nothing to order by but the coordinates
        (Some(mate_1), Some(mate_2)) => DupKey::PairSameStrand {
            left_5p: mate_1.five_prime.min(mate_2.five_prime),
            right_5p: mate_1.five_prime.max(mate_2.five_prime),
            reverse: mate_1.reverse,
        },
        (Some(mate), None) | (None, Some(mate)) => DupKey::OneMateAligned(mate),
        (None, None) => DupKey::NeitherAligned,
    };
    let avg_quality = average_quality_score(quality_fwd, quality_rev);
    // Return the ReadEntry with minimal memory footprint
    Ok(ReadEntry {
        query_name,
        genome_id: genome_id_sorted,
        key,
        avg_quality,
        attached: false,
    })
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
            make_read_entry(&fields, indices)
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
            let mut groups = build_groups_from_sorted_reads(reads, deviation);
            attach_lone_mates(&mut groups, deviation);
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
            // Find the exemplar using compare_reads, among the reads the group was
            // built from: an attached lone mate always loses, as in `samtools markdup`
            let exemplar = dup_group.iter()
                .filter(|read| !read.attached)
                .max_by(|a, b| compare_reads(a, b))
                .expect("a group is built from at least one unattached read");
            let exemplar_name = exemplar.query_name.clone();
            // Calculate size of duplicate group, attached lone mates included
            let dup_count = dup_group.len();
            // Calculate fraction of pairwise matches (as a QC metric for the group as a
            // whole), over the reads the group was built from: an attached lone mate
            // carries one coordinate and is not comparable to the pairs it joined
            let compared: Vec<&ReadEntry> =
                dup_group.iter().filter(|read| !read.attached).collect();
            let compared_count = compared.len();
            let pairwise_match_frac: f64;
            if compared_count <= 1 {
                pairwise_match_frac = 1.0;
            } else {
                // Stage 2 Multithreading: Parallel pairwise matching
                // Generate all pairs (i,j) where i < j and process them in parallel
                let compared_float: f64 = compared_count as f64;
                let n_pairs: f64 = compared_float * (compared_float - 1.0) / 2.0;
                // Use rayon to parallelize pairwise comparisons
                let pairwise_match_count: f64 = (0..compared_count)
                    .into_par_iter()  // Parallel iterator
                    .flat_map(|i| (i + 1..compared_count).into_par_iter().map(move |j| (i, j)))
                    .map(|(i, j)| {
                        let read_i = compared[i];
                        let read_j = compared[j];
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

    // The hits-table columns the fixtures carry.
    const HEADERS: [&str; 13] = [
        "seq_id",
        "prim_align_genome_id_all",
        "prim_align_ref_start",
        "prim_align_ref_start_rev",
        "prim_align_ref_start_unclipped",
        "prim_align_ref_start_unclipped_rev",
        "prim_align_ref_end_unclipped",
        "prim_align_ref_end_unclipped_rev",
        "prim_align_query_rc",
        "prim_align_query_rc_rev",
        "query_qual",
        "query_qual_rev",
        "prim_align_fragment_length",
    ];

    // One mate's fixture columns: unclipped start, unclipped end, and strand
    type Mate = (&'static str, &'static str, &'static str);
    // A mate that did not align carries no coordinates and no strand
    const UNALIGNED: Mate = ("NA", "NA", "NA");

    // A fixture row. Only the genome and the two mates decide the key, so the rest has
    // a default.
    struct Row {
        name: &'static str,
        genome: &'static str,
        mate_1: Mate,
        mate_2: Mate,
        qual: (&'static str, &'static str),
        // Each mate's clipped start, which this version does not read. None fills them
        // from the unclipped starts, i.e. models an alignment with nothing clipped.
        clipped_starts: Option<(&'static str, &'static str)>,
    }

    impl Default for Row {
        fn default() -> Self {
            Row {
                name: "r1",
                genome: "genome_a",
                mate_1: UNALIGNED,
                mate_2: UNALIGNED,
                qual: ("IIII", "IIII"),
                clipped_starts: None,
            }
        }
    }

    // Parse one fixture row, asserting that it is accepted
    fn parsed(row: Row) -> ReadEntry {
        parse(row).expect("row should parse")
    }

    // Parse one fixture row
    fn parse(row: Row) -> Result<ReadEntry, String> {
        let clipped = row.clipped_starts.unwrap_or((row.mate_1.0, row.mate_2.0));
        let values = [
            row.name,
            row.genome,
            clipped.0,
            clipped.1,
            row.mate_1.0,
            row.mate_2.0,
            row.mate_1.1,
            row.mate_2.1,
            row.mate_1.2,
            row.mate_2.2,
            row.qual.0,
            row.qual.1,
            "NA",
        ];
        assert_eq!(values.len(), HEADERS.len());
        let fields: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        let indices: HashMap<&'static str, usize> =
            HEADERS.iter().enumerate().map(|(i, &h)| (h, i)).collect();
        make_read_entry(&fields, &indices)
    }

    // The key an ordinary FR pair gets: its forward mate's 5' end, then its reverse
    // mate's
    fn pair(forward_5p: i32, reverse_5p: i32) -> DupKey {
        DupKey::PairOppositeStrands { forward_5p, reverse_5p }
    }

    // The key a lone aligned mate gets
    fn lone(five_prime: i32, reverse: bool) -> DupKey {
        DupKey::OneMateAligned(MateEnd { five_prime, reverse })
    }

    // Construct a ReadEntry directly, bypassing parsing. Grouping and matching tests use
    // this so they exercise the algorithm rather than the column layout.
    fn entry(name: &str, genome: &str, key: DupKey, quality: f64) -> ReadEntry {
        ReadEntry {
            query_name: name.to_string(),
            genome_id: genome.to_string(),
            key,
            avg_quality: quality,
            attached: false,
        }
    }

    // Groups come back in HashMap order, and reads within a group in sort order, so normalize
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
        assert_eq!(average_quality_score("III", "NA"), 20.0);
    }

    // --- Position comparison ---

    #[test]
    fn the_deviation_tolerance_is_inclusive_and_symmetric() {
        assert!(within(100, 100, 0));
        assert!(!within(100, 101, 0));
        // The boundary itself matches; one past it does not
        assert!(within(100, 101, 1));
        assert!(!within(100, 102, 1));
        assert!(within(100, 102, 2));
        assert!(!within(100, 103, 2));
        // Tolerance is symmetric
        assert!(within(102, 100, 2));
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
        let a = entry("a", "g", pair(10, 200), 30.0);
        let b = entry("b", "g", pair(20, 100), 30.0);
        let c = entry("c", "g", pair(10, 300), 30.0);
        // Start dominates, even when the end runs the other way
        assert_eq!(compare_read_coordinates(&a, &b), Ordering::Less);
        // Equal starts fall through to the end coordinate
        assert_eq!(compare_read_coordinates(&a, &c), Ordering::Less);
        assert_eq!(compare_read_coordinates(&a, &a), Ordering::Equal);
    }

    // --- Read entry construction ---

    #[test]
    fn make_read_entry_carries_name_genome_and_quality() {
        let e = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            qual: ("III", "!!!"),
            ..Row::default()
        });
        assert_eq!(e.query_name, "r1");
        assert_eq!(e.genome_id, "genome_a");
        assert_eq!(e.avg_quality, 20.0);
    }

    #[test]
    fn make_read_entry_keys_a_complete_pair_on_mate_five_prime_ends() {
        // A 150 bp FR pair spanning 500-949, keyed on the mates' 5' ends: mate 1's
        // unclipped start and mate 2's unclipped end.
        let e = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        assert_eq!(e.key, pair(500, 949));
    }

    #[test]
    fn make_read_entry_keys_a_complete_pair_the_same_regardless_of_order() {
        // Which mate is mate 1 is arbitrary, so the same fragment gives the same key
        let mate_1_leftmost = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        let mate_2_leftmost = parsed(Row {
            mate_1: ("800", "949", "True"),
            mate_2: ("500", "649", "False"),
            ..Row::default()
        });
        assert_eq!(mate_1_leftmost.key, mate_2_leftmost.key);
    }

    #[test]
    fn make_read_entry_keys_a_lone_aligned_mate_on_its_five_prime_end_and_strand() {
        let mate_1_aligned = parsed(Row {
            mate_1: ("500", "649", "False"),
            ..Row::default()
        });
        let mate_2_aligned = parsed(Row {
            name: "r2",
            mate_2: ("500", "649", "True"),
            ..Row::default()
        });
        // A forward mate's 5' end is its unclipped start, a reverse mate's its unclipped end
        assert_eq!(mate_1_aligned.key, lone(500, false));
        assert_eq!(mate_2_aligned.key, lone(649, true));
        // The two aligned to opposite strands, so they are no longer duplicates
        assert!(!match_reads(&mate_1_aligned, &mate_2_aligned, 0));
    }

    #[test]
    fn make_read_entry_keys_an_unaligned_pair_on_nothing() {
        let e = parsed(Row::default());
        assert_eq!(e.key, DupKey::NeitherAligned);
    }

    #[test]
    fn make_read_entry_sorts_split_genome_ids_and_their_mates_together() {
        // Mates on two genomes: the same pair of genomes always produces the same key
        // regardless of order.
        let e = parsed(Row {
            genome: "genome_b/genome_a",
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        assert_eq!(e.genome_id, "genome_a/genome_b");
        // genome_a is mate 2's genome here, so its mate leads
        assert_eq!(
            e.key,
            DupKey::SplitGenomes {
                first_mate: MateEnd { five_prime: 949, reverse: true },
                second_mate: MateEnd { five_prime: 500, reverse: false },
            }
        );

        let f = parsed(Row {
            name: "r2",
            genome: "genome_a/genome_b",
            mate_1: ("800", "949", "True"),
            mate_2: ("500", "649", "False"),
            ..Row::default()
        });
        assert_eq!(f.genome_id, "genome_a/genome_b");
        assert_eq!(e.key, f.key);
        assert!(match_reads(&e, &f, 0));

        // One mate on the other strand is a different molecule
        let flipped = parsed(Row {
            name: "r3",
            genome: "genome_b/genome_a",
            mate_1: ("500", "649", "True"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        assert!(!match_reads(&e, &flipped, 0));
    }


    #[test]
    fn make_read_entry_matches_copies_clipped_differently() {
        // Three copies of one fragment, clipped at different ends: the second 7 bases
        // off mate 1's leading end and the third 7 off mate 2's, each moving that
        // mate's POS by 7. The unclipped bounds are the same, so the keys are equal
        // at deviation 0.
        let pristine = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            clipped_starts: Some(("500", "800")),
            ..Row::default()
        });
        let clipped_leading = parsed(Row {
            name: "r2",
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            clipped_starts: Some(("507", "800")),
            ..Row::default()
        });
        let clipped_mate_2 = parsed(Row {
            name: "r3",
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            clipped_starts: Some(("500", "807")),
            ..Row::default()
        });
        assert_eq!(clipped_leading.key, pair(500, 949));
        assert_eq!(clipped_mate_2.key, pair(500, 949));
        assert!(match_reads(&pristine, &clipped_leading, 0));
        assert!(match_reads(&pristine, &clipped_mate_2, 0));
    }

    #[test]
    fn make_read_entry_matches_reverse_lone_mates_trimmed_to_different_lengths() {
        // Two copies of one molecule whose lone reverse mate was trimmed to different
        // lengths. POS is the 3' end and moves; the unclipped end does not.
        let long_copy = parsed(Row {
            mate_1: ("701", "800", "True"),
            ..Row::default()
        });
        let short_copy = parsed(Row {
            name: "r2",
            mate_1: ("711", "800", "True"),
            ..Row::default()
        });
        assert_eq!(long_copy.key, lone(800, true));
        assert!(match_reads(&long_copy, &short_copy, 0));
    }

    #[test]
    fn make_read_entry_separates_fragments_shorter_than_read() {
        // Both mates of a fragment shorter than the read report the same start, but
        // their 5' ends still differ by the fragment's length.
        let short = parsed(Row {
            mate_1: ("400", "439", "False"),
            mate_2: ("400", "439", "True"),
            ..Row::default()
        });
        let shorter = parsed(Row {
            name: "r2",
            mate_1: ("400", "429", "False"),
            mate_2: ("400", "429", "True"),
            ..Row::default()
        });
        assert_eq!(short.key, pair(400, 439));
        assert_eq!(shorter.key, pair(400, 429));
        assert!(!match_reads(&short, &shorter, 0));
    }

    #[test]
    fn make_read_entry_separates_pairs_with_different_orientations() {
        let fr = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        let ff = parsed(Row {
            name: "r2",
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "False"),
            ..Row::default()
        });
        assert_eq!(fr.key, pair(500, 949));
        assert_eq!(
            ff.key,
            DupKey::PairSameStrand { left_5p: 500, right_5p: 800, reverse: false }
        );
        assert!(!match_reads(&fr, &ff, 0));
    }

    #[test]
    fn make_read_entry_separates_fr_from_rf() {
        let fr = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        let rf = parsed(Row {
            name: "r2",
            mate_1: ("500", "649", "True"),
            mate_2: ("800", "949", "False"),
            ..Row::default()
        });
        assert_eq!(fr.key, pair(500, 949));
        assert_eq!(rf.key, pair(800, 649));
        assert!(!match_reads(&fr, &rf, 0));
    }

    #[test]
    fn make_read_entry_separates_ff_from_rr() {
        let ff = parsed(Row {
            mate_1: ("500", "649", "False"),
            mate_2: ("800", "949", "False"),
            ..Row::default()
        });
        let rr = parsed(Row {
            name: "r2",
            mate_1: ("500", "649", "True"),
            mate_2: ("800", "949", "True"),
            ..Row::default()
        });
        assert_eq!(
            ff.key,
            DupKey::PairSameStrand { left_5p: 500, right_5p: 800, reverse: false }
        );
        assert_eq!(
            rr.key,
            DupKey::PairSameStrand { left_5p: 649, right_5p: 949, reverse: true }
        );
        assert!(!match_reads(&ff, &rr, 0));
    }

    #[test]
    fn match_reads_tolerates_mates_within_the_deviation_of_each_other() {
        let a = parsed(Row {
            mate_1: ("400", "549", "False"),
            mate_2: ("400", "549", "True"),
            ..Row::default()
        });
        let b = parsed(Row {
            name: "r2",
            mate_1: ("401", "550", "False"),
            mate_2: ("400", "549", "True"),
            ..Row::default()
        });
        assert!(match_reads(&a, &b, 1));
    }

    #[test]
    fn make_read_entry_separates_unaligned_pairs_from_each_other() {
        let a = parsed(Row::default());
        let b = parsed(Row { name: "r2", ..Row::default() });
        assert!(!match_reads(&a, &b, 0));
    }

    #[test]
    fn make_read_entry_rejects_an_aligned_mate_with_no_strand() {
        // The strand decides which bound is the 5' end, so it cannot be defaulted
        let err = parse(Row {
            mate_1: ("500", "649", "NA"),
            ..Row::default()
        })
        .unwrap_err();
        assert!(err.contains("strand"), "unexpected error: {err}");
        assert!(err.contains("mate 1"), "unexpected error: {err}");
    }

    #[test]
    fn make_read_entry_rejects_half_an_unclipped_span() {
        let err = parse(Row {
            mate_2: ("500", "NA", "True"),
            ..Row::default()
        })
        .unwrap_err();
        assert!(
            err.contains("only one unclipped coordinate"),
            "unexpected error: {err}"
        );
        assert!(err.contains("mate 2"), "unexpected error: {err}");
    }

    #[test]
    fn make_read_entry_rejects_an_unreadable_coordinate() {
        // Not an integer and not NA, so keying the read as unaligned would be wrong
        for mate in [("x", "649", "False"), ("500", "x", "False")] {
            let err = parse(Row { mate_1: mate, ..Row::default() }).unwrap_err();
            assert!(err.contains("unreadable"), "unexpected error: {err}");
            assert!(err.contains("mate 1"), "unexpected error: {err}");
        }
    }

    // --- Header handling ---

    #[test]
    fn process_header_line_indexes_every_required_column() {
        let header = HEADERS.join("\t");
        let (headers, indices, count) = process_header_line(&header).unwrap();
        assert_eq!(count, HEADERS.len());
        assert_eq!(headers, HEADERS.to_vec());
        let unread = [
            "prim_align_ref_start",
            "prim_align_ref_start_rev",
            "prim_align_fragment_length",
        ];
        for required in HEADERS.iter().filter(|h| !unread.contains(h)) {
            assert!(indices.contains_key(required), "missing {required}");
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
        for missing in [
            "query_qual_rev",
            "prim_align_ref_start_unclipped",
            "prim_align_ref_end_unclipped_rev",
            "prim_align_query_rc",
        ] {
            let header = HEADERS
                .iter()
                .filter(|&&h| h != missing)
                .copied()
                .collect::<Vec<_>>()
                .join("\t");
            let err = process_header_line(&header).unwrap_err().to_string();
            assert!(
                err.contains("Missing required header"),
                "unexpected error: {err}"
            );
            assert!(err.contains(missing), "unexpected error: {err}");
        }
    }

    // --- Matching ---

    #[test]
    fn match_reads_requires_the_same_genome() {
        let a = entry("a", "genome_a", pair(100, 300), 30.0);
        let b = entry("b", "genome_b", pair(100, 300), 30.0);
        assert!(!match_reads(&a, &b, 2));
    }

    #[test]
    fn match_reads_never_compares_keys_of_different_kinds() {
        let keys = [
            pair(500, 800),
            DupKey::PairSameStrand { left_5p: 500, right_5p: 800, reverse: false },
            DupKey::SplitGenomes {
                first_mate: MateEnd { five_prime: 500, reverse: false },
                second_mate: MateEnd { five_prime: 800, reverse: true },
            },
            lone(500, false),
            DupKey::NeitherAligned,
        ];
        for (i, a) in keys.iter().enumerate() {
            for b in keys.iter().skip(i + 1) {
                let x = entry("x", "g", *a, 30.0);
                let y = entry("y", "g", *b, 30.0);
                assert!(!match_reads(&x, &y, 2), "{a:?} matched {b:?}");
            }
        }
    }

    #[test]
    fn match_reads_requires_both_coordinates_to_agree() {
        let a = entry("a", "g", pair(100, 300), 30.0);
        // Both within tolerance
        assert!(match_reads(&a, &entry("b", "g", pair(101, 301), 30.0), 1));
        // Start agrees, end does not
        assert!(!match_reads(&a, &entry("c", "g", pair(101, 310), 30.0), 1));
        // End agrees, start does not
        assert!(!match_reads(&a, &entry("d", "g", pair(110, 301), 30.0), 1));
    }

    // --- Exemplar selection ---

    #[test]
    fn compare_reads_ranks_by_quality_then_breaks_ties_on_name() {
        let high = entry("zzz", "g", pair(1, 2), 36.0);
        let low = entry("aaa", "g", pair(1, 2), 30.0);
        // Quality dominates, regardless of name
        assert_eq!(compare_reads(&high, &low), Ordering::Greater);
        // Equal quality falls back to the lexicographically smaller name winning
        let a = entry("aaa", "g", pair(1, 2), 30.0);
        let b = entry("bbb", "g", pair(1, 2), 30.0);
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
            entry("a", "g", pair(100, 300), 30.0),
            entry("b", "g", pair(101, 301), 30.0),
            entry("c", "g", pair(200, 400), 30.0),
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
            entry("c", "g", pair(200, 400), 30.0),
            entry("a", "g", pair(100, 300), 30.0),
            entry("b", "g", pair(101, 301), 30.0),
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
            entry("a", "g", pair(100, 300), 30.0),
            entry("b", "g", pair(101, 301), 30.0),
            entry("c", "g", pair(102, 302), 30.0),
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
            entry("a", "genome_a", pair(100, 300), 30.0),
            entry("b", "genome_b", pair(100, 300), 30.0),
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
            entry("a", "g", pair(100, 300), 30.0),
            entry("b", "g", pair(100, 900), 30.0),
        ];
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 1)),
            vec![vec!["a"], vec!["b"]]
        );
    }

    #[test]
    fn build_groups_from_sorted_reads_keeps_lone_mates_apart_by_strand() {
        let reads = vec![
            entry("a", "g", lone(100, false), 30.0),
            entry("b", "g", lone(100, true), 30.0),
            entry("c", "g", lone(100, false), 30.0),
            entry("d", "g", DupKey::NeitherAligned, 30.0),
            entry("e", "g", DupKey::NeitherAligned, 30.0),
        ];
        assert_eq!(
            group_names(build_groups_from_sorted_reads(reads, 1)),
            vec![vec!["a", "c"], vec!["b"], vec!["d"], vec!["e"]]
        );
    }

    // --- Attaching lone mates to pairs ---

    // Group the reads, attach the lone mates, and report the groups by name with the
    // attached reads marked
    fn attached_groups(reads: Vec<ReadEntry>, deviation: u8) -> Vec<Vec<String>> {
        let mut groups = build_groups_from_sorted_reads(reads, deviation);
        attach_lone_mates(&mut groups, deviation);
        let mut out: Vec<Vec<String>> = groups
            .into_iter()
            .map(|group| {
                let mut names: Vec<String> = group
                    .into_iter()
                    .map(|read| if read.attached {
                        format!("{}*", read.query_name)
                    } else {
                        read.query_name
                    })
                    .collect();
                names.sort();
                names
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn attach_lone_mates_marks_a_lone_mate_against_a_pair_on_its_five_prime_end() {
        let reads = vec![
            entry("p", "g", pair(500, 949), 36.0),
            entry("l", "g", lone(500, false), 30.0),
        ];
        assert_eq!(attached_groups(reads, 1), vec![vec!["l*", "p"]]);
    }

    #[test]
    fn attach_lone_mates_reaches_either_mate_of_a_pair() {
        // samtools keys both mates of a pair into its singles hash
        let reads = vec![
            entry("p", "g", pair(500, 949), 36.0),
            entry("l", "g", lone(949, true), 30.0),
        ];
        assert_eq!(attached_groups(reads, 1), vec![vec!["l*", "p"]]);
    }

    #[test]
    fn attach_lone_mates_requires_the_same_strand() {
        let reads = vec![
            entry("p", "g", pair(500, 949), 36.0),
            entry("l", "g", lone(500, true), 30.0),
        ];
        assert_eq!(attached_groups(reads, 1), vec![vec!["l"], vec!["p"]]);
    }

    #[test]
    fn attach_lone_mates_leaves_a_lone_mate_that_reaches_no_pair() {
        let reads = vec![
            entry("p", "g", pair(500, 949), 36.0),
            entry("l", "g", lone(600, false), 30.0),
        ];
        assert_eq!(attached_groups(reads, 1), vec![vec!["l"], vec!["p"]]);
    }

    #[test]
    fn attach_lone_mates_never_merges_two_groups_of_pairs() {
        // Both pairs start at 500, so the lone mate reaches both. It joins one of them
        // and the two pairs stay apart, which is what keeps a shared coordinate from
        // collapsing unrelated fragments.
        let reads = vec![
            entry("p1", "g", pair(500, 949), 36.0),
            entry("p2", "g", pair(500, 1200), 36.0),
            entry("l", "g", lone(500, false), 30.0),
        ];
        assert_eq!(attached_groups(reads, 1), vec![vec!["l*", "p1"], vec!["p2"]]);
    }

    #[test]
    fn attach_lone_mates_picks_the_nearest_pair_then_the_first_name() {
        let nearest = vec![
            entry("p1", "g", pair(498, 949), 36.0),
            entry("p2", "g", pair(501, 949), 36.0),
            entry("l", "g", lone(500, false), 30.0),
        ];
        assert_eq!(attached_groups(nearest, 2), vec![vec!["l*", "p2"], vec!["p1"]]);
        // Equidistant on either side, so the name decides and the choice does not
        // depend on the order the groups came out of the map
        let tied = vec![
            entry("p2", "g", pair(499, 949), 36.0),
            entry("p1", "g", pair(501, 949), 36.0),
            entry("l", "g", lone(500, false), 30.0),
        ];
        assert_eq!(attached_groups(tied, 1), vec![vec!["l*", "p1"], vec!["p2"]]);
    }

    #[test]
    fn attach_lone_mates_leaves_the_rest_of_a_cut_chain_in_one_group() {
        // The lone mates at 800-802 attach, taking with them the coordinates that
        // chained 799 to 803. The two left behind stay in the group they were built
        // into, so cutting a chain never produces an extra exemplar.
        let reads = vec![
            entry("p", "g", pair(801, 1200), 36.0),
            entry("l799", "g", lone(799, false), 30.0),
            entry("l800", "g", lone(800, false), 30.0),
            entry("l801", "g", lone(801, false), 30.0),
            entry("l802", "g", lone(802, false), 30.0),
            entry("l803", "g", lone(803, false), 30.0),
        ];
        assert_eq!(
            attached_groups(reads, 1),
            vec![vec!["l799", "l803"], vec!["l800*", "l801*", "l802*", "p"]]
        );
    }

    #[test]
    fn attach_lone_mates_reaches_same_strand_and_split_genome_pairs() {
        let same_strand = vec![
            entry("p", "g", DupKey::PairSameStrand { left_5p: 500, right_5p: 800, reverse: true }, 36.0),
            entry("l", "g", lone(800, true), 30.0),
        ];
        assert_eq!(attached_groups(same_strand, 1), vec![vec!["l*", "p"]]);
        let split = vec![
            entry("p", "g", DupKey::SplitGenomes {
                first_mate: MateEnd { five_prime: 500, reverse: false },
                second_mate: MateEnd { five_prime: 800, reverse: true },
            }, 36.0),
            entry("l", "g", lone(500, false), 30.0),
        ];
        assert_eq!(attached_groups(split, 1), vec![vec!["l*", "p"]]);
    }

    #[test]
    fn an_attached_lone_mate_never_becomes_the_exemplar() {
        // The lone mate scores higher, but samtools always marks the single read
        let mut group = vec![entry("pair", "g", pair(500, 949), 20.0)];
        let mut attached = entry("lone", "g", lone(500, false), 40.0);
        attached.attached = true;
        group.push(attached);
        let groups = HashMap::from([("g".to_string(), vec![group])]);
        let (exemplars, stats) = process_read_groups(groups, 1).unwrap();
        assert_eq!(exemplars["lone"].1, "pair");
        assert_eq!(stats[0].group_size, 2);
        // The attached read is not comparable to the pair, so it sits out the metric
        assert_eq!(stats[0].pairwise_match_frac, 1.0);
    }

    #[test]
    fn sort_start_and_sort_end_bound_every_key() {
        // The sliding window is bounded by the smallest coordinate a key holds
        assert_eq!((pair(800, 500).sort_start(), pair(800, 500).sort_end()), (Some(500), Some(800)));
        let same = DupKey::PairSameStrand { left_5p: 500, right_5p: 800, reverse: false };
        assert_eq!((same.sort_start(), same.sort_end()), (Some(500), Some(800)));
        assert_eq!((lone(500, true).sort_start(), lone(500, true).sort_end()), (Some(500), None));
        assert_eq!(
            (DupKey::NeitherAligned.sort_start(), DupKey::NeitherAligned.sort_end()),
            (None, None)
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
