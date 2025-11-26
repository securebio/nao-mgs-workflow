#!/usr/bin/env python

# TODO: Add unit tests for individual functions in a future pass

import pytest
import pandas as pd

import raise_taxonomy_ranks


class TestRaiseTaxonomyRanks:
    """Test the raise_taxonomy_ranks module."""

    def test_raise_ranks_on_test_data(self, tmp_path):
        """Test raising ranks on test data with expected output."""
        # Create test input DataFrame matching test-taxonomy-ranked.tsv structure
        input_data = {
            "taxid": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            "parent_taxid": ["-1", "0", "1", "2", "3", "4", "5", "6", "7", "5", "5"],
            "rank": ["acellular root", "kingdom", "phylum", "class", "order",
                    "family", "genus", "species", "subspecies", "none", "species"],
        }

        taxonomy_db = pd.DataFrame(input_data)
        taxonomy_db = taxonomy_db.set_index("taxid", drop=False)

        # Define target ranks
        target_ranks = ["species", "genus", "family"]

        # Store original column count
        original_col_count = taxonomy_db.shape[1]
        original_cols = taxonomy_db.columns.tolist()

        # Run the function
        result = raise_taxonomy_ranks.raise_ranks_db(taxonomy_db, target_ranks)

        # Verify structure
        assert result.shape[0] == len(input_data["taxid"])  # Same number of rows
        assert result.shape[1] == original_col_count + len(target_ranks)  # 3 extra columns

        # Verify original columns are preserved
        for col in original_cols:
            assert col in result.columns

        # Verify new columns exist
        for rank in target_ranks:
            new_col = f"taxid_{rank}"
            assert new_col in result.columns

        # Verify expected values (based on test-taxonomy-ranked.tsv)
        expected_species = [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, "7", "7", pd.NA, "10"]
        expected_genus = [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, "6", "6", "6", pd.NA, pd.NA]
        expected_family = [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, "5", "5", "5", "5", "5", "5"]

        # Convert pd.NA to string "NA" for comparison (matching CSV output)
        result_species = result["taxid_species"].fillna("NA").tolist()
        result_genus = result["taxid_genus"].fillna("NA").tolist()
        result_family = result["taxid_family"].fillna("NA").tolist()

        expected_species_str = ["NA" if pd.isna(v) else v for v in expected_species]
        expected_genus_str = ["NA" if pd.isna(v) else v for v in expected_genus]
        expected_family_str = ["NA" if pd.isna(v) else v for v in expected_family]

        assert result_species == expected_species_str
        assert result_genus == expected_genus_str
        assert result_family == expected_family_str
