"""Shared pytest fixtures for local module tests."""

import gzip
import os
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tsv_factory(tmp_path):
    """Factory fixture for TSV file operations.

    Provides methods to create and read both plain and gzipped TSV files.

    Example:
        def test_something(tsv_factory):
            input_file = tsv_factory.create_plain("input.tsv", "col1\\tcol2\\nval1\\tval2\\n")
            output_file = tsv_factory.get_path("output.tsv")
            # ... run function ...
            result = tsv_factory.read_plain(output_file)
    """

    class TSVFactory:
        def __init__(self, tmp_path):
            self.tmp_path = tmp_path

        def create_plain(self, filename, content):
            """Create a plain TSV file with the given content."""
            file_path = self.tmp_path / filename
            file_path.write_text(content)
            return str(file_path)

        def create_gzip(self, filename, content):
            """Create a gzipped TSV file with the given content."""
            file_path = self.tmp_path / filename
            with gzip.open(file_path, "wt") as f:
                f.write(content)
            return str(file_path)

        def read_plain(self, filepath):
            """Read content from a plain TSV file."""
            return Path(filepath).read_text()

        def read_gzip(self, filepath):
            """Read content from a gzipped TSV file."""
            with gzip.open(filepath, "rt") as f:
                return f.read()

        def get_path(self, filename):
            """Get the full path for a file in the temp directory."""
            return str(self.tmp_path / filename)

    return TSVFactory(tmp_path)


@pytest.fixture
def common_tsv_data():
    """Common TSV test datasets used across multiple test modules.

    Provides standard test data patterns like empty files, header-only files,
    basic multi-column data, etc.
    """
    return {
        "basic_3col": "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6\n",
        "basic_2col": "col1\tcol2\nval1\tval2\nval3\tval4\n",
        "empty": "",
        "header_only": "col1\tcol2\tcol3\n",
        "single_column": "col1\nval1\nval2\nval3\n",
        "single_row": "col1\tcol2\tcol3\nval1\tval2\tval3\n",
        "with_duplicates": "id\tname\tvalue\n1\talice\t10\n1\tbob\t20\n",
        "whitespace_only": "\n\n\n",
    }


@pytest.fixture
def temp_file_helper():
    """Helper for temporary file operations with automatic cleanup.

    Provides methods for creating TSV files from headers/rows and managing
    temporary files without manual cleanup.

    Example:
        def test_something(temp_file_helper):
            input_file = temp_file_helper.create_tsv(
                "input.tsv",
                ["col1", "col2"],
                [["val1", "val2"], ["val3", "val4"]]
            )
            result = temp_file_helper.read_file(input_file)
    """

    class TempFileHelper:
        def __init__(self):
            self.temp_dir = tempfile.mkdtemp()

        def create_tsv(self, filename, header, rows):
            """Create a TSV file from header and rows."""
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, "w") as f:
                f.write("\t".join(header) + "\n")
                for row in rows:
                    f.write("\t".join(row) + "\n")
            return filepath

        def create_file(self, filename, content):
            """Create a file with the given content."""
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, "w") as f:
                f.write(content)
            return filepath

        def read_file(self, filepath):
            """Read content from a file."""
            with open(filepath, "r") as f:
                return f.read()

        def read_tsv_lines(self, filepath):
            """Read TSV file and return lines as a list."""
            with open(filepath, "r") as f:
                return [line.strip() for line in f if line.strip()]

        def get_path(self, filename):
            """Get the full path for a file in the temp directory."""
            return os.path.join(self.temp_dir, filename)

        def cleanup(self):
            """Clean up all temporary files."""
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)

    helper = TempFileHelper()
    yield helper
    helper.cleanup()
