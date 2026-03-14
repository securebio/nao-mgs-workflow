#!/usr/bin/env Rscript

library(jsonlite)
library(optparse)
library(tidyverse)

# Set arguments
option_list = list(
  make_option(c("-i", "--input_dir"), type="character", default=NULL,
              help="Path to multiqc data directory."),
  make_option(c("-s", "--stage"), type="character", default=NULL,
              help="Stage descriptor."),
  make_option(c("-S", "--sample"), type="character", default=NULL,
              help="Sample ID."),
  make_option(c("-r", "--single_end"), type="character", default=FALSE,
              help="Single-end flag."),
  make_option(c("-o", "--output_dir"), type="character", default=NULL,
              help="Path to output directory."),
  make_option(c("-p", "--prefix"), type="character", default=NULL,
              help="Output file prefix (if NULL, uses sample name).")
)
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

# Convert single_end from string to logical
if (opt$single_end == "true") {
  single_end <- TRUE
} else if (opt$single_end == "false") {
  single_end <- FALSE
} else {
  stop("single_end must be 'true' or 'false'")
}

# Set input paths
multiqc_json_path <- file.path(opt$input_dir, "multiqc_data.json")
fastqc_tsv_path <- file.path(opt$input_dir, "multiqc_fastqc.txt")

# Set output paths
# Format: {prefix}_qc_{type}_stats_{stage}.tsv.gz (e.g. sample1_qc_basic_stats_raw.tsv.gz)
if (is.null(opt$prefix)) {
  prefix <- opt$sample
} else {
  prefix <- opt$prefix
}
out_path_basic <- file.path(opt$output_dir, paste0(prefix, "_qc_basic_stats_", opt$stage, ".tsv.gz"))
out_path_adapters <- file.path(opt$output_dir, paste0(prefix, "_qc_adapter_stats_", opt$stage, ".tsv.gz"))
out_path_quality_base <- file.path(opt$output_dir, paste0(prefix, "_qc_quality_base_stats_", opt$stage, ".tsv.gz"))
out_path_quality_sequence <- file.path(opt$output_dir, paste0(prefix, "_qc_quality_sequence_stats_", opt$stage, ".tsv.gz"))
out_path_lengths <- file.path(opt$output_dir, paste0(prefix, "_qc_length_stats_", opt$stage, ".tsv.gz"))

#=====================#
# AUXILIARY FUNCTIONS #
#=====================#

process_n_bases <- function(n_bases_vec){
  # Function for extracting approximate base-count information from FASTQC TSV
  val = n_bases_vec %>% str_split(" ") %>% sapply(first) %>% as.numeric
  unit = n_bases_vec %>% str_split(" ") %>% sapply(last)
  # Adjust val based on unit
  val_out = ifelse(unit == "Gbp", val * 10^9, val) # TODO: Add other units as they come up
  val_out = ifelse(unit == "Mbp", val_out * 10^6, val_out)
  val_out = ifelse(unit == "kbp", val_out * 10^3, val_out)
  return(val_out)
}

basic_info_fastqc <- function(fastqc_tsv, multiqc_json, single_end){
  # Read in basic stats from multiqc JSON
  # MultiQC 1.33+ nests stats under module names: {module: {sample: {stats}}}
  stats_json <- multiqc_json$report_general_stats_data
  all_samples <- list()
  for (module in names(stats_json)) {
    for (sample in names(stats_json[[module]])) {
      all_samples[[sample]] <- c(all_samples[[sample]], stats_json[[module]][[sample]])
    }
  }
  tab_json <- lapply(names(all_samples), function(x) {
    as_tibble(all_samples[[x]]) %>% mutate(file = x)
  }) %>% bind_rows() %>%
    summarize(percent_gc = mean(percent_gc),
              mean_seq_len = mean(avg_sequence_length),
              n_reads_single = sum(total_sequences),
              n_read_pairs = ifelse(single_end, NA, sum(total_sequences) / 2),
              percent_duplicates = mean(percent_duplicates))
  # Read in basic stats from fastqc TSV
  columns_exclude <- c("Sample", "Filename", "File type", "Encoding", "Total Sequences", "Total Bases",
                      "Sequences flagged as poor quality", "Sequence length", "%GC",
                      "total_deduplicated_percentage", "basic_statistics", "avg_sequence_length",
                      "median_sequence_length")
  
  tab_tsv <- fastqc_tsv %>%
    mutate(n_bases_approx = process_n_bases(`Total Bases`) %>% as.numeric) %>%
    select(-any_of(columns_exclude)) %>%
    select(n_bases_approx, everything()) %>%
    summarize_all(function(x) paste(x, collapse="/"))
  
  # Ensure per_base_sequence_quality and per_sequence_quality_scores are present 
  # (they are missing from multiqc JSON if multiqc was run on empty file, but we always want them)
  required_columns <- c("per_base_sequence_quality", "per_tile_sequence_quality", "per_sequence_quality_scores")
  missing_cols <- setdiff(required_columns, colnames(tab_tsv))
  if (length(missing_cols) > 0) {
    tab_tsv[missing_cols] <- NA
  } 
  
  return(bind_cols(tab_json, tab_tsv))
}

extract_plot_lines <- function(multiqc_json, plot_id, col_names){
  # Extract line data from a MultiQC 1.33+ plot, returning a tibble.
  # col_names should be a length-2 vector naming the x and y columns.
  plot_data <- multiqc_json$report_plot_data[[plot_id]]
  if (is.null(plot_data)) return(NULL)
  # jsonlite parses datasets as a data frame; lines is a list column.
  # Access the first dataset's lines: datasets$lines[[1]]
  n_datasets <- if (is.data.frame(plot_data$datasets)) nrow(plot_data$datasets) else length(plot_data$datasets$lines)
  if (n_datasets > 1) {
    warning(sprintf("Plot '%s' has %d datasets; only the first is used", plot_id, n_datasets))
  }
  lines <- plot_data$datasets$lines[[1]]
  if (is.null(lines)) return(NULL)
  # jsonlite may simplify the lines array into:
  # (a) a data frame with $name and $pairs columns
  # (b) a named list with $name (vector) and $pairs (list) — parallel structure
  # (c) a list of individual line objects (each with $name and $pairs)
  # Cases (a) and (b) both have lines$name as non-NULL; iterate by index.
  # Case (c) has lines$name as NULL; iterate over elements.
  if (!is.null(lines$name)) {
    n <- if (is.data.frame(lines)) nrow(lines) else length(lines$name)
    pairs <- lines$pairs
    data_out <- lapply(1:n, function(i) {
      # jsonlite may simplify pairs into a 3D array when all samples have
      # the same number of data points; use array slicing in that case
      if (is.array(pairs) && length(dim(pairs)) == 3) {
        p <- pairs[i, , ]
      } else {
        p <- pairs[[i]]
      }
      as.data.frame(p) %>% setNames(col_names) %>% mutate(file = lines$name[i])
    }) %>% bind_rows() %>% as_tibble()
  } else {
    data_out <- lapply(lines, function(line) {
      as.data.frame(line$pairs) %>% setNames(col_names) %>% mutate(file = line$name)
    }) %>% bind_rows() %>% as_tibble()
  }
  return(data_out)
}

extract_adapter_data <- function(multiqc_json){
  # Extract adapter data from multiqc JSON
  data_out <- extract_plot_lines(multiqc_json, "fastqc_adapter_content_plot",
                                  c("position", "pc_adapters"))
  if (is.null(data_out) || nrow(data_out) == 0){
    return(tibble(file = character(), position = numeric(),
                  adapter = character(), pc_adapters = numeric()))
  }
  # Adapter name is encoded in the line name as "file - adapter"
  data_out <- data_out %>%
    rename(filename = file) %>%
    separate_wider_delim("filename", " - ", names=c("file", "adapter")) %>%
    select(file, position, adapter, pc_adapters)
  return(data_out)
}

extract_length_data <- function(multiqc_json){
  # Extract length data from multiqc JSON
  data_out <- extract_plot_lines(multiqc_json, "fastqc_sequence_length_distribution_plot",
                                  c("length", "n_sequences"))
  if (is.null(data_out) || nrow(data_out) == 0){
    # Fallback for uniform read lengths (no length distribution plot)
    stats <- multiqc_json$report_general_stats_data$fastqc
    if (is.null(stats)){
      warning("extract_length_data: no length distribution plot found and ",
              "'fastqc' key missing from report_general_stats_data; ",
              "returning empty result")
      return(tibble(length = numeric(), n_sequences = numeric(), file = character()))
    }
    tab_out <- lapply(names(stats), function(x) {
      tibble(length = stats[[x]]$avg_sequence_length,
             n_sequences = stats[[x]]$total_sequences,
             file = x)
    }) %>% bind_rows()
    return(tab_out)
  }
  return(data_out)
}

extract_per_base_quality <- function(multiqc_json){
  # Extract per-base sequence quality data from multiqc JSON
  data_out <- extract_plot_lines(multiqc_json, "fastqc_per_base_sequence_quality_plot",
                                  c("position", "mean_phred_score"))
  if (is.null(data_out) || nrow(data_out) == 0){
    return(tibble(position = numeric(), mean_phred_score = numeric(), file = character()))
  }
  return(data_out)
}

extract_per_sequence_quality <- function(multiqc_json){
  # Extract per-sequence quality data from multiqc JSON
  data_out <- extract_plot_lines(multiqc_json, "fastqc_per_sequence_quality_scores_plot",
                                  c("mean_phred_score", "n_sequences"))
  if (is.null(data_out) || nrow(data_out) == 0){
    return(tibble(mean_phred_score = numeric(), n_sequences = numeric(), file = character()))
  }
  return(data_out)
}

#============#
# RUN SCRIPT #
#============#

# Import data
multiqc_json_lines <- readLines(multiqc_json_path)
multiqc_json_lines_sub <- gsub("NaN", "-1", multiqc_json_lines)
multiqc_json <- fromJSON(multiqc_json_lines_sub)
fastqc_tsv <- readr::read_tsv(fastqc_tsv_path, show_col_types = FALSE)

# Process
add_info <- function(tab) mutate(tab, stage=opt$stage, sample=opt$sample)
basic_info <- basic_info_fastqc(fastqc_tsv, multiqc_json, single_end) %>% add_info
adapters <- extract_adapter_data(multiqc_json) %>% add_info
per_base_quality <- extract_per_base_quality(multiqc_json) %>% add_info
lengths <- extract_length_data(multiqc_json) %>% add_info
per_sequence_quality <- extract_per_sequence_quality(multiqc_json) %>% add_info

# Write tables
write_tsv(basic_info, out_path_basic)
write_tsv(adapters, out_path_adapters)
write_tsv(per_base_quality, out_path_quality_base)
write_tsv(per_sequence_quality, out_path_quality_sequence)
write_tsv(lengths, out_path_lengths)
