#!/usr/bin/env Rscript

#library(jsonlite)
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
              help="Output file prefix (if NULL, uses stage_sample).")
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
  stats_json <- multiqc_json$report_general_stats_data
  tab_json <- lapply(names(stats_json),
                     function(x) stats_json[[x]] %>% mutate(file=x)) %>% bind_rows() %>%
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

extract_adapter_data_single <- function(adapter_dataset){
  # Convert a single JSON adapter dataset into a tibble
  data <- lapply(1:length(adapter_dataset$name), function(n)
    adapter_dataset$data[[n]] %>% as.data.frame %>%
      mutate(filename=adapter_dataset$name[n])) %>%
    bind_rows() %>% as_tibble %>%
    rename(position=V1, pc_adapters=V2) %>%
    separate_wider_delim("filename", " - ", names=c("file", "adapter"))
  return(data)
}

extract_adapter_data <- function(multiqc_json){
  # Extract adapter data from multiqc JSON
  datasets <- multiqc_json$report_plot_data$fastqc_adapter_content_plot$datasets$lines
  data_out <- lapply(datasets, extract_adapter_data_single) %>% bind_rows()
  # Make sure all columns are present even if no adapters
  if (nrow(data_out) == 0){
      data_out <- data_out %>% mutate(file = character(), position = numeric(),
                                      adapter = character(), pc_adapters = numeric())
  }
  return(data_out)
}

extract_length_data_single <- function(length_dataset){
  # Convert a single JSON length dataset into a tibble
  data <- lapply(1:length(length_dataset$name), function(n)
    length_dataset$data[[n]] %>% as.data.frame %>%
      mutate(filename=length_dataset$name[n])) %>%
    bind_rows() %>% as_tibble %>%
    rename(length=V1, n_sequences=V2) %>%
    rename(file = filename)
  return(data)
}

extract_length_data <- function(multiqc_json){
  # Extract length data from multiqc JSON
  datasets <- multiqc_json$report_plot_data$fastqc_sequence_length_distribution_plot$datasets$lines
  if (is.null(datasets) || length(datasets) == 0){
    datasets <- multiqc_json$report_general_stats_headers$avg_sequence_length$dmax
    filename <- names(multiqc_json$report_saved_raw_data$multiqc_general_stats)
    seq_num <- sapply(filename, function(x) multiqc_json$report_saved_raw_data$multiqc_general_stats[[x]]$`FastQC_mqc-generalstats-fastqc-total_sequences`)
    tab_out <- tibble(length=datasets, n_sequences=seq_num, file=filename)
    return(tab_out)
  }
  data_out <- lapply(datasets, extract_length_data_single) %>% bind_rows()
  return(data_out)
}

extract_per_base_quality_single <- function(per_base_quality_dataset){
  # Convert a single JSON per-base-quality dataset into a tibble
  data <- lapply(1:length(per_base_quality_dataset$name), function(n)
    per_base_quality_dataset$data[[n]] %>% as.data.frame %>%
      mutate(file=per_base_quality_dataset$name[n])) %>%
    bind_rows() %>% as_tibble %>%
    rename(position=V1, mean_phred_score=V2)
  return(data)
}

extract_per_base_quality <- function(multiqc_json){
  # Extract per-base sequence quality data from multiqc JSON
  datasets <- multiqc_json$report_plot_data$fastqc_per_base_sequence_quality_plot$datasets$lines
  data_out <- lapply(datasets, extract_per_base_quality_single) %>% bind_rows()
  # Make sure all columns are present even if no quality data
  if (nrow(data_out) == 0){
    data_out <- data_out %>% mutate(file = character(), position = numeric(),
                                    mean_phred_score = numeric())
  }
  return(data_out)
}

extract_per_sequence_quality_single <- function(per_sequence_quality_dataset){
  # Convert a single JSON per-sequence-quality dataset into a tibble
  data <- lapply(1:length(per_sequence_quality_dataset$name), function(n)
    per_sequence_quality_dataset$data[[n]] %>% as.data.frame %>%
      mutate(file=per_sequence_quality_dataset$name[n])) %>%
    bind_rows() %>% as_tibble %>%
    rename(mean_phred_score=V1, n_sequences=V2)
  return(data)
}

extract_per_sequence_quality <- function(multiqc_json){
  # Extract per-base sequence quality data from multiqc JSON
  datasets <- multiqc_json$report_plot_data$fastqc_per_sequence_quality_scores_plot$datasets$lines
  data_out <- lapply(datasets, extract_per_sequence_quality_single) %>% bind_rows()
  # Make sure all columns are present even if no quality data
  if (nrow(data_out) == 0){
    data_out <- data_out %>% mutate(file = character(), mean_phred_score = numeric(),
                                    n_sequences = numeric())
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
