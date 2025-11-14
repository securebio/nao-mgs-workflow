// Make TSV of taxid, name, rank and parent taxid for all viruses in NCBI taxonomy
process BUILD_VIRUS_TAXID_DB {
    label "single"
    label "rpkg"
    input:
        path(taxonomy_nodes)
        path(taxonomy_names)
        val(virus_taxid)
    output:
        path("total-virus-db.tsv.gz"), emit: db
    shell:
        '''
        #!/usr/bin/env Rscript

        # Setup
        library(tidyverse)
        nodes_path <- "!{taxonomy_nodes}"
        names_path <- "!{taxonomy_names}"
        virus_taxid <- "!{virus_taxid}"
        out_path   <- "total-virus-db.tsv.gz"

        # Import NCBI data (NB: convoluted method for names file to avoid problems from unpaired quotes)
        nodes <- read_tsv(nodes_path, col_names = FALSE) %>%
            transmute(taxid = X1, parent_taxid = X3, rank = X5)
        names <- names_path %>% read_file %>% gsub("\\"", "", .) %>%
            read_tsv(col_names = FALSE) %>%
            transmute(taxid = X1, name = X3, alt_name = X5, type = X7)

        # Get complete list of viral taxids
        taxids_out <- virus_taxid # Start with taxid for all viruses
        taxids_new <- filter(nodes, parent_taxid %in% taxids_out, !taxid %in% taxids_out) %>%
            pull(taxid) %>% sort
        while (length(taxids_new) > 0){
            taxids_out <- c(taxids_out, taxids_new) %>% sort %>% unique
            taxids_new <- filter(nodes, parent_taxid %in% taxids_out, !taxid %in% taxids_out) %>%
                pull(taxid) %>% sort
        }

        # Build output DB from nodes and names
        virus_db <- filter(nodes, taxid %in% taxids_out) %>%
            left_join(names, by="taxid") %>% select(-alt_name)
        virus_db_scinames <- filter(virus_db, type == "scientific name") %>%
            group_by(taxid) %>% filter(row_number() == 1) %>% select(-type)
        virus_db_other <- filter(virus_db, !taxid %in% virus_db_scinames$taxid) %>%
            group_by(taxid) %>% filter(row_number() == 1) %>% select(-type)
        virus_db_out <- bind_rows(virus_db_scinames, virus_db_other) %>%
            select(taxid, name, rank, parent_taxid) %>% arrange(taxid) %>%
            mutate(rank = replace_na(rank, "no rank"),
                   name = replace_na(name, "unknown virus"))

        # Write output
        write_tsv(virus_db_out, out_path)
        '''
}
