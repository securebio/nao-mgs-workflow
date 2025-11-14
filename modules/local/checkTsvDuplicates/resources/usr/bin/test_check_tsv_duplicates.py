#!/usr/bin/env python

import pytest

import check_tsv_duplicates


class TestCheckDuplicates:
    """Test the check_duplicates function."""

    @pytest.mark.parametrize(
        "file_format",
        ["plain", "gzip"],
        ids=["plain_tsv", "gzipped_tsv"],
    )
    def test_no_duplicates(self, tsv_factory, file_format):
        """Test file with no duplicates passes successfully."""
        input_content = "id\tname\tvalue\n1\talice\t10\n2\tbob\t20\n3\tcharlie\t30\n"
        expected = "id\tname\tvalue\n1\talice\t10\n2\tbob\t20\n3\tcharlie\t30\n"

        if file_format == "plain":
            input_file = tsv_factory.create_plain("input.tsv", input_content)
            output_file = tsv_factory.get_path("output.tsv")
        else:
            input_file = tsv_factory.create_gzip("input.tsv.gz", input_content)
            output_file = tsv_factory.get_path("output.tsv.gz")

        check_tsv_duplicates.check_duplicates(input_file, output_file, "id")

        if file_format == "plain":
            result = tsv_factory.read_plain(output_file)
        else:
            result = tsv_factory.read_gzip(output_file)

        assert result == expected

    @pytest.mark.parametrize(
        "input_content,field,expected_match",
        [
            (
                "id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n",
                "id",
                "Duplicate value found",
            ),
            (
                "id\tname\tvalue\n3\tcharlie\t30\n1\talice\t10\n",
                "id",
                "File is not sorted",
            ),
            (
                "id\tname\tvalue\n1\talice\t10\n",
                "missing_field",
                "Field not found in header",
            ),
            (
                "",
                "id",
                "No header to select fields from",
            ),
        ],
        ids=["duplicate_values", "unsorted_file", "missing_field", "empty_header"],
    )
    def test_check_duplicates_errors(
        self, tsv_factory, input_content, field, expected_match
    ):
        """Test various error conditions for check_duplicates."""
        input_file = tsv_factory.create_plain("input.tsv", input_content)
        output_file = tsv_factory.get_path("output.tsv")

        with pytest.raises(ValueError, match=expected_match):
            check_tsv_duplicates.check_duplicates(input_file, output_file, field)
