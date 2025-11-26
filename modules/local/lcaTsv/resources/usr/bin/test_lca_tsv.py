"""
Test module for lca_tsv.py
"""

import gzip
import os
import pytest
import sys

# Import the module being tested
import lca_tsv


class TestLcaTsv:
    """Test cases for lca_tsv module"""

    @pytest.fixture
    def toy_taxonomy_dir(self):
        """Fixture providing path to toy taxonomy data."""
        return os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..", "..", "..",
            "test-data", "toy-data", "lca-taxonomy"
        )

    @pytest.fixture
    def toy_nodes_db(self, toy_taxonomy_dir):
        """Fixture providing path to toy nodes database."""
        return os.path.join(toy_taxonomy_dir, "test-nodes.dmp")

    @pytest.fixture
    def toy_names_db(self, toy_taxonomy_dir):
        """Fixture providing path to toy names database."""
        return os.path.join(toy_taxonomy_dir, "test-names.dmp")

    @pytest.fixture
    def expected_headers(self):
        """Fixture providing expected output headers."""
        return [
            "seq_id",
            "test_taxid_lca_combined", "test_n_assignments_combined", "test_n_assignments_classified_combined",
            "test_taxid_top_combined", "test_taxid_top_classified_combined",
            "test_test_score_min_combined", "test_test_score_max_combined", "test_test_score_mean_combined",
            "test_taxid_lca", "test_n_assignments", "test_n_assignments_classified",
            "test_taxid_top", "test_taxid_top_classified",
            "test_test_score_min", "test_test_score_max", "test_test_score_mean",
            "test_taxid_lca_artificial", "test_n_assignments_artificial", "test_n_assignments_classified_artificial",
            "test_taxid_top_artificial", "test_taxid_top_classified_artificial",
            "test_test_score_min_artificial", "test_test_score_max_artificial", "test_test_score_mean_artificial"
        ]

    @pytest.fixture
    def natural_input_data(self):
        """Fixture providing natural input test data."""
        return (
            "seq_id\ttaxid\ttest_score\texp_lca\texp_top_taxid\tcomments\n"
            "A\t9001\t100\t9001\t9001\tsingle match\n"
            "B\t9002\t100\t9000\t9003\ttwo matches\n"
            "B\t9003\t200\t9000\t9003\ttwo matches\n"
            "C\t9004\t100\t9004\t9004\ttwo identical matches\n"
            "C\t9004\t150\t9004\t9004\ttwo identical matches\n"
            "D\t9005\t100\t9003\t9007\tthree close matches\n"
            "D\t9006\t200\t9003\t9007\tthree close matches\n"
            "D\t9007\t300\t9003\t9007\tthree close matches\n"
            "E\t9005\t100\t9000\t9008\tthree distant matches\n"
            "E\t9006\t200\t9000\t9008\tthree distant matches\n"
            "E\t9008\t300\t9000\t9008\tthree distant matches\n"
            "F\t9001\t100\t1\t9999\tone match, one missing\n"
            "F\t9999\t150\t1\t9999\tone match, one missing\n"
        )

    @pytest.fixture
    def artificial_input_data(self):
        """Fixture providing artificial input test data."""
        return (
            "seq_id\ttaxid\ttest_score\texp_lca_artificial\texp_lca\texp_lca_combined\tcomments\n"
            "X\t8001\t100\t8000\tNA\t8000\ttwo artificial matches\n"
            "X\t8002\t200\t8000\tNA\t8000\ttwo artificial matches\n"
            "Y\t8001\t100\t8001\t9001\t1\tone natural & one artificial match\n"
            "Y\t9001\t100\t8001\t9001\t1\tone natural & one artificial match\n"
            "Z\t9001\t100\t8000\t9000\t1\ttwo natural & two artificial matches\n"
            "Z\t9002\t100\t8000\t9000\t1\ttwo natural & two artificial matches\n"
            "Z\t8001\t100\t8000\t9000\t1\ttwo natural & two artificial matches\n"
            "Z\t8002\t100\t8000\t9000\t1\ttwo natural & two artificial matches\n"
        )

    @pytest.fixture
    def unclassified_input_data(self):
        """Fixture providing unclassified input test data."""
        return (
            "seq_id\ttaxid\ttest_score\texp_lca\texp_top_taxid\texp_top_taxid_classified\texp_n_classified\tcomment\n"
            "Q\t9005\t200\t9004\t9005\tTrue\t2\tnon-top unclassified match\n"
            "Q\t9006\t200\t9004\t9005\tTrue\t2\tnon-top unclassified match\n"
            "Q\t9009\t50\t9004\t9005\tTrue\t2\tnon-top unclassified match\n"
            "R\t9005\t200\t9000\t9009\tFalse\t2\ttop unclassified match\n"
            "R\t9006\t200\t9000\t9009\tFalse\t2\ttop unclassified match\n"
            "R\t9009\t260\t9000\t9009\tFalse\t2\ttop unclassified match\n"
            "S\t9005\t200\t9003\t9010\tFalse\t2\tnon-tied unclassified matches\n"
            "S\t9006\t200\t9003\t9010\tFalse\t2\tnon-tied unclassified matches\n"
            "S\t9009\t200\t9003\t9010\tFalse\t2\tnon-tied unclassified matches\n"
            "S\t9010\t240\t9003\t9010\tFalse\t2\tnon-tied unclassified matches\n"
            "T\t9005\t200\t9000\t9009\tFalse\t2\ttied unclassified matches\n"
            "T\t9006\t200\t9000\t9009\tFalse\t2\ttied unclassified matches\n"
            "T\t9009\t240\t9000\t9009\tFalse\t2\ttied unclassified matches\n"
            "T\t9010\t240\t9000\t9009\tFalse\t2\ttied unclassified matches\n"
        )

    @pytest.fixture
    def taxonomy_dbs(self, toy_nodes_db, toy_names_db):
        """Fixture providing loaded taxonomy databases."""
        child_to_parent, parent_to_children = lca_tsv.parse_nodes_db(toy_nodes_db, 8000)
        names_db = lca_tsv.parse_names_db(toy_names_db)
        unclassified_taxids = lca_tsv.get_unclassified_taxids(names_db)
        unclassified_taxids_descendants = lca_tsv.get_descendants(unclassified_taxids, parent_to_children)
        artificial_taxids = lca_tsv.get_descendants(set([8000]), parent_to_children)

        return {
            "child_to_parent": child_to_parent,
            "parent_to_children": parent_to_children,
            "artificial_taxids": artificial_taxids,
            "unclassified_taxids_descendants": unclassified_taxids_descendants
        }

    def test_natural_input(self, tsv_factory, natural_input_data, taxonomy_dbs, expected_headers, tmp_path):
        """Test LCA computation on natural input."""
        input_file = tsv_factory.create_plain("input.tsv", natural_input_data)
        output_file = str(tmp_path / "output.tsv.gz")

        # Run LCA computation
        lca_tsv.parse_input_tsv(
            input_file, output_file, "seq_id", "taxid", "test_score",
            taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
            taxonomy_dbs["unclassified_taxids_descendants"], "test"
        )

        # Read output
        with gzip.open(output_file, "rt") as f:
            output_lines = [line.strip().split("\t") for line in f.readlines()]

        # Parse input to get expected values per group
        input_lines = [line.strip().split("\t") for line in natural_input_data.strip().split("\n")[1:]]
        groups = {}
        for fields in input_lines:
            group_id = fields[0]
            if group_id not in groups:
                groups[group_id] = []
            groups[group_id].append({
                "taxid": int(fields[1]),
                "score": float(fields[2]),
                "exp_lca": int(fields[3]),
                "exp_top_taxid": int(fields[4])
            })

        # Verify output
        headers = output_lines[0]
        assert len(output_lines) == len(groups) + 1  # header + one row per group
        assert headers == expected_headers

        # Check each group's output
        for i, (group_id, entries) in enumerate(sorted(groups.items())):
            row = output_lines[i + 1]
            assert row[0] == group_id

            # Expected values
            exp_lca = entries[0]["exp_lca"]
            exp_top_taxid = entries[0]["exp_top_taxid"]
            exp_n_entries = len(entries)
            exp_min_score = min(e["score"] for e in entries)
            exp_max_score = max(e["score"] for e in entries)
            exp_mean_score = sum(e["score"] for e in entries) / exp_n_entries

            # All entries are natural (not artificial)
            assert row[headers.index("test_taxid_lca")] == str(exp_lca)
            assert row[headers.index("test_n_assignments")] == str(exp_n_entries)
            assert row[headers.index("test_taxid_top")] == str(exp_top_taxid)
            assert float(row[headers.index("test_test_score_min")]) == exp_min_score
            assert float(row[headers.index("test_test_score_max")]) == exp_max_score
            assert float(row[headers.index("test_test_score_mean")]) == exp_mean_score

            # Artificial fields should be NA
            assert row[headers.index("test_taxid_lca_artificial")] == "NA"
            assert row[headers.index("test_n_assignments_artificial")] == "0"

    def test_artificial_input(self, tsv_factory, artificial_input_data, taxonomy_dbs, expected_headers, tmp_path):
        """Test LCA computation with mixed natural and artificial sequences."""
        input_file = tsv_factory.create_plain("input.tsv", artificial_input_data)
        output_file = str(tmp_path / "output.tsv.gz")

        # Run LCA computation
        lca_tsv.parse_input_tsv(
            input_file, output_file, "seq_id", "taxid", "test_score",
            taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
            taxonomy_dbs["unclassified_taxids_descendants"], "test"
        )

        # Read output
        with gzip.open(output_file, "rt") as f:
            output_lines = [line.strip().split("\t") for line in f.readlines()]

        # Parse expected values from input
        input_lines = [line.strip().split("\t") for line in artificial_input_data.strip().split("\n")[1:]]
        groups = {}
        for fields in input_lines:
            group_id = fields[0]
            if group_id not in groups:
                groups[group_id] = {"exp_lca_artificial": None, "exp_lca": None, "exp_lca_combined": None}
            # Use first entry's expected values
            if groups[group_id]["exp_lca_artificial"] is None:
                groups[group_id]["exp_lca_artificial"] = fields[3] if fields[3] != "NA" else "NA"
                groups[group_id]["exp_lca"] = fields[4] if fields[4] != "NA" else "NA"
                groups[group_id]["exp_lca_combined"] = fields[5]

        # Verify output
        headers = output_lines[0]
        assert len(output_lines) == len(groups) + 1
        assert headers == expected_headers

        # Check LCA values for each group
        for i, group_id in enumerate(sorted(groups.keys())):
            row = output_lines[i + 1]
            assert row[0] == group_id

            exp = groups[group_id]
            assert row[headers.index("test_taxid_lca_combined")] == exp["exp_lca_combined"]
            assert row[headers.index("test_taxid_lca")] == exp["exp_lca"]
            assert row[headers.index("test_taxid_lca_artificial")] == exp["exp_lca_artificial"]

    def test_unclassified_input(self, tsv_factory, unclassified_input_data, taxonomy_dbs, expected_headers, tmp_path):
        """Test LCA computation with unclassified taxids."""
        input_file = tsv_factory.create_plain("input.tsv", unclassified_input_data)
        output_file = str(tmp_path / "output.tsv.gz")

        # Run LCA computation
        lca_tsv.parse_input_tsv(
            input_file, output_file, "seq_id", "taxid", "test_score",
            taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
            taxonomy_dbs["unclassified_taxids_descendants"], "test"
        )

        # Read output
        with gzip.open(output_file, "rt") as f:
            output_lines = [line.strip().split("\t") for line in f.readlines()]

        # Parse expected values from input
        input_lines = [line.strip().split("\t") for line in unclassified_input_data.strip().split("\n")[1:]]
        groups = {}
        for fields in input_lines:
            group_id = fields[0]
            if group_id not in groups:
                groups[group_id] = []
            groups[group_id].append({
                "exp_lca": int(fields[3]),
                "exp_top_taxid": int(fields[4]),
                "exp_top_taxid_classified": fields[5] == "True",
                "exp_n_classified": int(fields[6]),
                "score": float(fields[2])
            })

        # Verify output
        headers = output_lines[0]
        assert len(output_lines) == len(groups) + 1
        assert headers == expected_headers

        # Check values for each group
        for i, (group_id, entries) in enumerate(sorted(groups.items())):
            row = output_lines[i + 1]
            assert row[0] == group_id

            # Use first entry's expected values (they're the same for all entries in a group)
            exp = entries[0]
            exp_n_entries = len(entries)
            exp_min_score = min(e["score"] for e in entries)
            exp_max_score = max(e["score"] for e in entries)
            exp_mean_score = sum(e["score"] for e in entries) / exp_n_entries

            assert row[headers.index("test_taxid_lca")] == str(exp["exp_lca"])
            assert row[headers.index("test_taxid_top")] == str(exp["exp_top_taxid"])
            assert row[headers.index("test_taxid_top_classified")] == str(exp["exp_top_taxid_classified"])
            assert row[headers.index("test_n_assignments")] == str(exp_n_entries)
            assert row[headers.index("test_n_assignments_classified")] == str(exp["exp_n_classified"])
            assert float(row[headers.index("test_test_score_min")]) == exp_min_score
            assert float(row[headers.index("test_test_score_max")]) == exp_max_score
            assert float(row[headers.index("test_test_score_mean")]) == exp_mean_score

    def test_missing_group_field(self, tsv_factory, taxonomy_dbs, tmp_path):
        """Test that missing group field raises assertion error."""
        input_content = "seq_id\ttaxid\ttest_score\nA\t9001\t100\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        with pytest.raises(AssertionError, match="Group field not found in header"):
            lca_tsv.parse_input_tsv(
                input_file, output_file, "missing", "taxid", "test_score",
                taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
                taxonomy_dbs["unclassified_taxids_descendants"], "test"
            )

    def test_missing_taxid_field(self, tsv_factory, taxonomy_dbs, tmp_path):
        """Test that missing taxid field raises assertion error."""
        input_content = "seq_id\ttaxid\ttest_score\nA\t9001\t100\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        with pytest.raises(AssertionError, match="Taxid field not found in header"):
            lca_tsv.parse_input_tsv(
                input_file, output_file, "seq_id", "missing", "test_score",
                taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
                taxonomy_dbs["unclassified_taxids_descendants"], "test"
            )

    def test_missing_score_field(self, tsv_factory, taxonomy_dbs, tmp_path):
        """Test that missing score field raises assertion error."""
        input_content = "seq_id\ttaxid\ttest_score\nA\t9001\t100\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        with pytest.raises(AssertionError, match="Score field not found in header"):
            lca_tsv.parse_input_tsv(
                input_file, output_file, "seq_id", "taxid", "missing",
                taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
                taxonomy_dbs["unclassified_taxids_descendants"], "test"
            )

    def test_missing_root_in_taxonomy_db(self, tsv_factory, toy_taxonomy_dir, tmp_path):
        """Test that missing root in taxonomy DB raises assertion error."""
        input_content = "seq_id\ttaxid\ttest_score\nA\t9001\t100\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        # Use truncated nodes DB that's missing root
        truncated_nodes_db = os.path.join(toy_taxonomy_dir, "test-nodes-truncated.dmp")

        with pytest.raises(AssertionError, match="Taxonomy DB does not contain root"):
            child_to_parent, parent_to_children = lca_tsv.parse_nodes_db(truncated_nodes_db, 8000)

    def test_unsorted_input(self, tsv_factory, taxonomy_dbs, tmp_path):
        """Test that unsorted input raises assertion error."""
        # This input has groups out of order (B comes after C)
        input_content = (
            "seq_id\ttaxid\ttest_score\texp_lca\tcomments\n"
            "A\t9001\t100\t9001\tsingle match\n"
            "B\t9002\t100\t9000\ttwo matches\n"
            "C\t9004\t100\t9004\ttwo identical matches\n"
            "B\t9003\t200\t9000\ttwo matches\n"
            "C\t9004\t150\t9004\ttwo identical matches\n"
        )
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        with pytest.raises(AssertionError, match="Group ID out of order"):
            lca_tsv.parse_input_tsv(
                input_file, output_file, "seq_id", "taxid", "test_score",
                taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
                taxonomy_dbs["unclassified_taxids_descendants"], "test"
            )

    def test_empty_file_no_header(self, tsv_factory, taxonomy_dbs, tmp_path):
        """Test that fully empty file produces empty output."""
        input_content = ""
        input_file = tsv_factory.create_plain("input.txt", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        lca_tsv.parse_input_tsv(
            input_file, output_file, "seq_id", "taxid", "test_score",
            taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
            taxonomy_dbs["unclassified_taxids_descendants"], "test"
        )

        # Output file should be empty (no lines)
        with gzip.open(output_file, "rt") as f:
            content = f.read()
        assert content == ""

    def test_empty_file_header_only(self, tsv_factory, taxonomy_dbs, expected_headers, tmp_path):
        """Test that header-only file produces header-only output."""
        input_content = "seq_id\ttaxid\ttest_score\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = str(tmp_path / "output.tsv.gz")

        lca_tsv.parse_input_tsv(
            input_file, output_file, "seq_id", "taxid", "test_score",
            taxonomy_dbs["child_to_parent"], taxonomy_dbs["artificial_taxids"],
            taxonomy_dbs["unclassified_taxids_descendants"], "test"
        )

        # Output should have header only
        with gzip.open(output_file, "rt") as f:
            lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "\t".join(expected_headers)
