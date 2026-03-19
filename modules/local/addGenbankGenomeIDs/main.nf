// Add genome ID information to Genbank metadata table
process ADD_GENBANK_GENOME_IDS {
    label "biopython"
    label "single"
    input:
        path(genbank_metadata)
        path(genbank_genomes)
        val(filename_prefix)
    output:
        path("${filename_prefix}-metadata-gid.tsv.gz")
    script:
        """
        #!/usr/bin/env python
        # Import packages
        import json
        import gzip
        import pandas as pd
        from Bio.SeqIO.FastaIO import SimpleFastaParser
        # Import metadata and get filepaths
        meta_db = pd.read_csv("${genbank_metadata}", sep="\\t", dtype=str)
        filepaths = meta_db["local_filename"]
        # Iterate over filepaths to extract genome IDs
        gid_lists = []
        for path in filepaths:
            gid_list = []
            with gzip.open(path, "rt") as inf:
                for title, sequence in SimpleFastaParser(inf):
                    genome_id, name = title.split(" ", 1)
                    gid_list.append(genome_id)
            gid_lists.append(gid_list)
        # Expand metadata table and add genome IDs
        expanded_data = []
        for idx, value_list in enumerate(gid_lists):
            for value in value_list:
                expanded_data.append((idx, value))
        indices, values = zip(*expanded_data)
        meta_db_gid = meta_db.iloc[list(indices)].copy()
        meta_db_gid["genome_id"] = values
        # Write output
        out_path = "${filename_prefix}-metadata-gid.tsv.gz"
        meta_db_gid.to_csv(out_path, sep="\\t", index=False)
        """
}
