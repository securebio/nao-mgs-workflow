#!/usr/bin/env python

# TODO: Add unit tests for individual functions (parse_nodes_db, path_to_root,
# compute_lca, compute_taxonomic_distance, etc.) in a future pass

import pytest

import compute_taxid_distance


class TestComputeTaxidDistance:
    """Test the compute_taxid_distance module."""

    @pytest.fixture
    def test_nodes_db(self, tsv_factory):
        """Create a test taxonomy nodes database."""
        content = (
            "1\t|\t1\n"
            "9000\t|\t1\n"
            "9001\t|\t9000\n"
            "9002\t|\t9000\n"
            "9003\t|\t9000\n"
            "9004\t|\t9003\n"
            "9005\t|\t9004\n"
            "9006\t|\t9004\n"
            "9007\t|\t9003\n"
            "9008\t|\t9001\n"
            "9009\t|\t9008\n"
            "9010\t|\t9003\n"
            "8000\t|\t1\n"
            "8001\t|\t8000\n"
            "8002\t|\t8000\n"
        )
        return tsv_factory.create_plain("nodes.dmp", content)

    @pytest.fixture
    def truncated_nodes_db(self, tsv_factory):
        """Create a truncated taxonomy nodes database (missing root)."""
        content = (
            "9000\t|\t9999\n"
            "9001\t|\t9000\n"
        )
        return tsv_factory.create_plain("nodes_truncated.dmp", content)

    def test_missing_taxid_field(self, tsv_factory, test_nodes_db):
        """Test that missing taxid field raises ValueError."""
        input_file = tsv_factory.create_plain(
            "input.tsv",
            "x\ty\tz\n0\t1\t2\n3\t4\t5\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        field_names = {
            "taxid_1": "x",
            "taxid_2": "a",  # Field 'a' doesn't exist
            "distance_1": "distance_1",
            "distance_2": "distance_2"
        }

        child_to_parent, _ = compute_taxid_distance.parse_nodes_db(test_nodes_db)

        with pytest.raises(ValueError, match="Field not found in header: a"):
            compute_taxid_distance.process_input_to_output(
                input_file,
                output_file,
                field_names,
                child_to_parent
            )

    def test_distance_field_1_already_exists(self, tsv_factory, test_nodes_db):
        """Test that existing distance field 1 raises ValueError."""
        input_file = tsv_factory.create_plain(
            "input.tsv",
            "x\ty\tz\tw\n0\t1\t2\t3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        field_names = {
            "taxid_1": "x",
            "taxid_2": "y",
            "distance_1": "z",  # Field 'z' already exists
            "distance_2": "w"
        }

        child_to_parent, _ = compute_taxid_distance.parse_nodes_db(test_nodes_db)

        with pytest.raises(ValueError, match="Distance field already present in input header"):
            compute_taxid_distance.process_input_to_output(
                input_file,
                output_file,
                field_names,
                child_to_parent
            )

    def test_distance_field_2_already_exists(self, tsv_factory, test_nodes_db):
        """Test that existing distance field 2 raises ValueError."""
        input_file = tsv_factory.create_plain(
            "input.tsv",
            "x\ty\tz\tw\n0\t1\t2\t3\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        field_names = {
            "taxid_1": "x",
            "taxid_2": "y",
            "distance_1": "w",
            "distance_2": "z"  # Field 'z' already exists
        }

        child_to_parent, _ = compute_taxid_distance.parse_nodes_db(test_nodes_db)

        with pytest.raises(ValueError, match="Distance field already present in input header"):
            compute_taxid_distance.process_input_to_output(
                input_file,
                output_file,
                field_names,
                child_to_parent
            )

    def test_missing_root_in_taxonomy_db(self, tsv_factory, truncated_nodes_db):
        """Test that missing root in taxonomy DB raises AssertionError."""
        with pytest.raises(AssertionError, match="Taxonomy DB does not contain root"):
            compute_taxid_distance.parse_nodes_db(truncated_nodes_db)

    def test_empty_input_file_no_header(self, tsv_factory, test_nodes_db):
        """Test that empty input file (no header) raises ValueError."""
        input_file = tsv_factory.create_plain("input.tsv", "")
        output_file = tsv_factory.get_path("output.tsv")

        field_names = {
            "taxid_1": "x",
            "taxid_2": "a",
            "distance_1": "distance_1",
            "distance_2": "distance_2"
        }

        child_to_parent, _ = compute_taxid_distance.parse_nodes_db(test_nodes_db)

        with pytest.raises(ValueError, match="Header line is empty: no fields to parse"):
            compute_taxid_distance.process_input_to_output(
                input_file,
                output_file,
                field_names,
                child_to_parent
            )

    def test_header_only_input(self, tsv_factory, test_nodes_db):
        """Test that input file with header only produces header-only output."""
        input_file = tsv_factory.create_plain(
            "input.tsv",
            "x\ty\n"
        )
        output_file = tsv_factory.get_path("output.tsv")

        field_names = {
            "taxid_1": "x",
            "taxid_2": "y",
            "distance_1": "distance_1",
            "distance_2": "distance_2"
        }

        child_to_parent, _ = compute_taxid_distance.parse_nodes_db(test_nodes_db)

        compute_taxid_distance.process_input_to_output(
            input_file,
            output_file,
            field_names,
            child_to_parent
        )

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Should have exactly one line (header)
        assert len(lines) == 1

        # Output header should have 2 additional fields
        input_headers = ["x", "y"]
        output_headers = lines[0].split("\t")
        assert len(output_headers) == len(input_headers) + 2

        # Output header should match input header plus distance fields
        assert output_headers[:-2] == input_headers
        assert output_headers[-2] == "distance_1"
        assert output_headers[-1] == "distance_2"

    @pytest.mark.parametrize(
        "taxid1,taxid2,exp_dist1,exp_dist2,comment",
        [
            ("9000", "9000", "0", "0", "Matching taxids"),
            ("9001", "9000", "1", "0", "Child/parent"),
            ("9000", "9001", "0", "1", "Parent/child"),
            ("9004", "9000", "2", "0", "Grandchild/grandparent"),
            ("9000", "9004", "0", "2", "Grandparent/grandchild"),
            ("9001", "9002", "1", "1", "Siblings"),
            ("9007", "9009", "2", "3", "Distant cousins"),
            ("None", "9000", "NA", "NA", "Invalid taxid"),
        ],
    )
    def test_valid_input_cases(
        self,
        tsv_factory,
        test_nodes_db,
        taxid1,
        taxid2,
        exp_dist1,
        exp_dist2,
        comment
    ):
        """Test computation of taxonomic distances for various taxid pairs."""
        input_content = f"taxid1\ttaxid2\tcomment\n{taxid1}\t{taxid2}\t{comment}\n"
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        field_names = {
            "taxid_1": "taxid1",
            "taxid_2": "taxid2",
            "distance_1": "distance_1",
            "distance_2": "distance_2"
        }

        child_to_parent, _ = compute_taxid_distance.parse_nodes_db(test_nodes_db)

        compute_taxid_distance.process_input_to_output(
            input_file,
            output_file,
            field_names,
            child_to_parent
        )

        result = tsv_factory.read_plain(output_file)
        lines = result.strip().split("\n")

        # Should have header + 1 data row
        assert len(lines) == 2

        # Check header
        headers = lines[0].split("\t")
        assert "taxid1" in headers
        assert "taxid2" in headers
        assert "distance_1" in headers
        assert "distance_2" in headers

        # Check data row
        data = lines[1].split("\t")
        dist1_idx = headers.index("distance_1")
        dist2_idx = headers.index("distance_2")

        assert data[dist1_idx] == exp_dist1
        assert data[dist2_idx] == exp_dist2

        # Original columns should be preserved
        taxid1_idx = headers.index("taxid1")
        taxid2_idx = headers.index("taxid2")
        assert data[taxid1_idx] == taxid1
        assert data[taxid2_idx] == taxid2
