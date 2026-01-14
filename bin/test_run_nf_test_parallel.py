#!/usr/bin/env python3
"""
Unit tests for run-nf-test-parallel.py

Run with: pytest bin/test_run-nf-test-parallel.py
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import subprocess

# Import functions from the script
import sys
sys.path.insert(0, str(Path(__file__).parent))
from run_nf_test_parallel import (
    find_test_files,
    divide_test_files,
    extract_failures_from_output,
    strip_ansi_codes,
    execute_subprocess,
    run_nf_test_worker,
    update_plugins,
    run_parallel_tests,
    write_test_log,
)

class TestStripAnsiCodes:
    """Test ANSI escape code removal."""

    def test_strip_complex_codes(self):
        """Test removal of complex ANSI sequences."""
        text = "\x1B[1;32;40mBold green on black\x1B[0m"
        assert strip_ansi_codes(text) == "Bold green on black"

    def test_no_ansi_codes(self):
        """Test text without ANSI codes remains unchanged."""
        text = "Plain text"
        assert strip_ansi_codes(text) == "Plain text"

    def test_multiple_codes(self):
        """Test text with multiple ANSI codes."""
        text = "\x1B[31mRed\x1B[0m and \x1B[32mGreen\x1B[0m"
        assert strip_ansi_codes(text) == "Red and Green"

class TestFindTestFiles:
    """Test finding *.nf.test files."""

    def test_find_single_file(self, tmp_path):
        """Test finding a single .nf.test file."""
        test_file = tmp_path / "test.nf.test"
        test_file.touch()
        result = find_test_files([str(test_file)])
        assert len(result) == 1
        assert result[0] == test_file

    def test_find_files_in_directory(self, tmp_path):
        """Test finding multiple .nf.test files in a directory."""
        (tmp_path / "test1.nf.test").touch()
        (tmp_path / "test2.nf.test").touch()
        (tmp_path / "other.txt").touch()
        result = find_test_files([str(tmp_path)])
        assert len(result) == 2
        assert all(f.name.endswith('.nf.test') for f in result)

    def test_find_files_recursively(self, tmp_path):
        """Test recursive search in nested directories."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "test1.nf.test").touch()
        (subdir / "test2.nf.test").touch()
        result = find_test_files([str(tmp_path)])
        assert len(result) == 2

    def test_nonexistent_path(self, tmp_path):
        """Test handling of nonexistent paths."""
        result = find_test_files([str(tmp_path / "nonexistent")])
        assert len(result) == 0

    def test_mixed_files_and_directories(self, tmp_path):
        """Test finding files from mixed file and directory paths."""
        test_file = tmp_path / "direct.nf.test"
        test_file.touch()
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        indirect_file = subdir / "indirect.nf.test"
        indirect_file.touch()
        result = find_test_files([str(test_file), str(subdir)])
        assert len(result) == 2
        assert sorted(result) == sorted([test_file, indirect_file])

    def test_sorted_output(self, tmp_path):
        """Test that output is sorted."""
        (tmp_path / "c.nf.test").touch()
        (tmp_path / "a.nf.test").touch()
        (tmp_path / "b.nf.test").touch()
        result = find_test_files([str(tmp_path)])
        names = [f.name for f in result]
        assert names == sorted(names)

class TestDivideTestFiles:
    """Test dividing test files among workers."""

    @pytest.mark.parametrize("n_files,n_workers", [
        (6, 3), (9, 3), (4, 2), (8, 4),  # Even distribution
        (5, 3), (7, 3), (10, 4), (11, 5),  # Uneven distribution
        (2, 5), (1, 3), (3, 10),  # More workers than files
        (0, 3), (5, 1), (1, 1), (100, 7),  # Edge cases
    ])
    def test_divide_distribution(self, n_files, n_workers):
        """Test file distribution with various combinations."""
        files = [Path(f"test{i}.nf.test") for i in range(n_files)]
        result = divide_test_files(files, n_workers)
        assert len(result) == n_workers
        base_count = n_files // n_workers if n_workers > 0 else 0
        remainder = n_files % n_workers if n_workers > 0 else 0
        for worker_idx, worker_files in enumerate(result):
            expected_count = base_count + (1 if worker_idx < remainder else 0)
            assert len(worker_files) == expected_count
            assert worker_files == files[worker_idx::n_workers]
        all_assigned_files = [f for worker in result for f in worker]
        assert len(all_assigned_files) == n_files
        assert set(all_assigned_files) == set(files)

class TestExecuteSubprocess:
    """Test subprocess execution wrapper."""

    @patch('run_nf_test_parallel.subprocess.run')
    def test_execute_subprocess(self, mock_run):
        """Test that execute_subprocess correctly wraps subprocess.run."""
        mock_result = MagicMock()
        mock_result.returncode = 42
        mock_result.stdout = "stdout content"
        mock_result.stderr = "stderr content"
        mock_run.return_value = mock_result
        exit_code, stdout, stderr = execute_subprocess(["test", "command"])
        assert exit_code == mock_result.returncode
        assert stdout == mock_result.stdout
        assert stderr == mock_result.stderr
        mock_run.assert_called_once_with(["test", "command"], capture_output=True, text=True)

class TestRunNfTestWorker:
    """Test nf-test worker function."""

    def test_worker_with_no_files(self):
        """Test worker with no assigned test files."""
        worker_id, exit_code, stdout, stderr, cmd_str = run_nf_test_worker(
            worker_id=1,
            test_files=[],
            total_workers=3,
            debug=False
        )
        assert worker_id == 1
        assert exit_code == 0
        assert stdout == ""
        assert stderr == ""
        assert cmd_str == ""

    @pytest.mark.parametrize("test_files,expected_exit,mock_stdout,mock_stderr", [
        ([Path("test1.nf.test")], 0, "Test passed\n", ""),
        ([Path("test1.nf.test"), Path("test2.nf.test"), Path("test3.nf.test")], 0, "All tests passed\n", ""),
        ([Path("failing_test.nf.test")], 1, "FAILED (1.0s)\n", "Error message"),
    ])
    @patch('run_nf_test_parallel.execute_subprocess')
    def test_worker_with_files(self, mock_execute, test_files, expected_exit, mock_stdout, mock_stderr):
        """Test worker with various file configurations and outcomes."""
        mock_execute.return_value = (expected_exit, mock_stdout, mock_stderr)
        worker_id, exit_code, stdout, stderr, cmd_str = run_nf_test_worker(
            worker_id=1,
            test_files=test_files,
            total_workers=2,
            debug=False
        )
        assert worker_id == 1
        assert exit_code == expected_exit
        assert stdout == mock_stdout
        assert stderr == mock_stderr
        exp_call_args = ["nf-test", "test"] + [str(test_file) for test_file in test_files]
        assert cmd_str == " ".join(exp_call_args)
        mock_execute.assert_called_once()
        assert mock_execute.call_args[0][0] == exp_call_args

    @patch('run_nf_test_parallel.execute_subprocess')
    def test_worker_with_debug(self, mock_execute):
        """Test worker with debug mode."""
        mock_execute.return_value = (0, "test1", "test2")
        test_files = [Path("test1.nf.test"), Path("test2.nf.test")]
        worker_id, exit_code, stdout, stderr, cmd_str = run_nf_test_worker(
            worker_id=1,
            test_files=test_files,
            total_workers=2,
            debug=True
        )
        assert worker_id == 1
        assert exit_code == 0
        assert stdout == "test1"
        assert stderr == "test2"
        exp_call_args = ["nf-test", "test", "--debug", "--verbose"] + [str(test_file) for test_file in test_files]
        assert cmd_str == " ".join(exp_call_args)
        mock_execute.assert_called_once()
        assert mock_execute.call_args[0][0] == exp_call_args

class TestExtractFailuresFromOutput:
    """Test extracting FAILED test names from output."""

    def test_extract_failures(self):
        """Test extracting failures from output."""
        lines = ["Test 1", "FAILED (1.0s)", "Test 2", "PASSED (0.5s)", "Test 3", "FAILED (2.0s)"]
        input = "\n".join(lines)
        exp_result = [line for line in lines if "FAILED" in line]
        result = extract_failures_from_output(input)
        assert result == exp_result

class TestUpdatePlugins:
    """Test nf-test plugin update function."""

    @patch('run_nf_test_parallel.subprocess.run')
    def test_update_plugins_success(self, mock_run):
        """Test successful plugin update."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        update_plugins()
        mock_run.assert_called_once_with(
            ["nf-test", "update-plugins"],
            capture_output=True,
            text=True,
            timeout=120
        )

    @patch('run_nf_test_parallel.subprocess.run')
    def test_update_plugins_failure(self, mock_run):
        """Test plugin update with non-zero exit code."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Plugin update failed"
        mock_run.return_value = mock_result
        with pytest.raises(RuntimeError, match="Failed to update plugins"):
            update_plugins()

    @patch('run_nf_test_parallel.subprocess.run')
    def test_update_plugins_timeout(self, mock_run):
        """Test plugin update timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="nf-test", timeout=120)
        with pytest.raises(RuntimeError, match="Plugin update timed out after 120 seconds"):
            update_plugins()

class TestRunParallelTests:
    """Test parallel test execution orchestration."""

    @patch('run_nf_test_parallel.write_test_log')
    @patch('run_nf_test_parallel.multiprocessing.Pool')
    @patch('run_nf_test_parallel.divide_test_files')
    @patch('run_nf_test_parallel.update_plugins')
    @patch('run_nf_test_parallel.find_test_files')
    def test_run_parallel_tests_no_files(self, mock_find, mock_update, mock_divide, mock_pool, mock_write):
        """Test when no test files are found."""
        mock_find.return_value = []
        with pytest.raises(RuntimeError, match="No test files found"):
            run_parallel_tests(2, ["tests"], Path("test-logs.txt"), debug=False)
        mock_find.assert_called_once_with(["tests"])
        mock_update.assert_not_called()

    @patch('run_nf_test_parallel.write_test_log')
    @patch('run_nf_test_parallel.multiprocessing.Pool')
    @patch('run_nf_test_parallel.divide_test_files')
    @patch('run_nf_test_parallel.update_plugins')
    @patch('run_nf_test_parallel.find_test_files')
    def test_run_parallel_tests_success(self, mock_find, mock_update, mock_divide, mock_pool, mock_write):
        """Test successful parallel execution."""
        test_files = [Path("test1.nf.test"), Path("test2.nf.test")]
        mock_find.return_value = test_files
        mock_divide.return_value = [[test_files[0]], [test_files[1]]]
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.starmap.return_value = [
            (1, 0, "stdout1", "stderr1", "cmd1"),
            (2, 0, "stdout2", "stderr2", "cmd2"),
        ]
        exit_code = run_parallel_tests(2, ["tests"], Path("test-logs.txt"), debug=False)
        assert exit_code == 0
        mock_find.assert_called_once_with(["tests"])
        mock_update.assert_called_once()
        mock_divide.assert_called_once_with(test_files, 2)
        mock_write.assert_called_once()

    @patch('run_nf_test_parallel.write_test_log')
    @patch('run_nf_test_parallel.multiprocessing.Pool')
    @patch('run_nf_test_parallel.divide_test_files')
    @patch('run_nf_test_parallel.update_plugins')
    @patch('run_nf_test_parallel.find_test_files')
    def test_run_parallel_tests_with_failures(self, mock_find, mock_update, mock_divide, mock_pool, mock_write):
        """Test parallel execution with some worker failures."""
        test_files = [Path("test1.nf.test"), Path("test2.nf.test")]
        mock_find.return_value = test_files
        mock_divide.return_value = [[test_files[0]], [test_files[1]]]
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.starmap.return_value = [
            (1, 0, "stdout1", "stderr1", "cmd1"),
            (2, 1, "stdout2", "stderr2", "cmd2"),
        ]
        exit_code = run_parallel_tests(2, ["tests"], Path("test-logs.txt"), debug=False)
        assert exit_code == 1
        mock_write.assert_called_once()

    @patch('run_nf_test_parallel.write_test_log')
    @patch('run_nf_test_parallel.multiprocessing.Pool')
    @patch('run_nf_test_parallel.divide_test_files')
    @patch('run_nf_test_parallel.update_plugins')
    @patch('run_nf_test_parallel.find_test_files')
    def test_run_parallel_tests_adjusts_workers(self, mock_find, mock_update, mock_divide, mock_pool, mock_write):
        """Test that number of workers is reduced when it exceeds number of test files."""
        test_files = [Path("test1.nf.test"), Path("test2.nf.test")]
        mock_find.return_value = test_files
        mock_divide.return_value = [[test_files[0]], [test_files[1]]]
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.starmap.return_value = [
            (1, 0, "stdout1", "stderr1", "cmd1"),
            (2, 0, "stdout2", "stderr2", "cmd2"),
        ]
        exit_code = run_parallel_tests(5, ["tests"], Path("test-logs.txt"), debug=False)
        assert exit_code == 0
        mock_divide.assert_called_once_with(test_files, 2)
