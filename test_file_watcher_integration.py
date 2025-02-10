#!/usr/bin/env python3
import unittest
import tempfile
import shutil
import os
import time
import threading
from pathlib import Path
import logging
import subprocess
import signal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestFileWatcherIntegration(unittest.TestCase):
    def setUp(self):
        # Create temporary test directories
        self.source_dir = tempfile.mkdtemp()
        self.target_dir = tempfile.mkdtemp()
        logger.info(f"Test directories created: {self.source_dir} -> {self.target_dir}")

        # Start the file watcher in a separate process
        self.process = subprocess.Popen(
            [
                "python3",
                "main.py",
                self.source_dir,
                self.target_dir
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Give it time to start up
        time.sleep(2)
        logger.info("File watcher started")

    def tearDown(self):
        # Stop the file watcher
        logger.info("Stopping file watcher")
        self.process.send_signal(signal.SIGTERM)
        stdout, stderr = self.process.communicate(timeout=5)
        logger.info(f"File watcher output: {stdout.decode()}")
        if stderr:
            logger.error(f"File watcher errors: {stderr.decode()}")

        # Clean up test directories
        shutil.rmtree(self.source_dir)
        shutil.rmtree(self.target_dir)
        logger.info("Test directories cleaned up")

    def test_basic_file_operations(self):
        """Test creating, modifying, and deleting files"""
        # Create a test file
        test_file = Path(self.source_dir) / "test.txt"
        with open(test_file, "w") as f:
            f.write("initial content")
        logger.info(f"Created test file: {test_file}")

        # Wait for file to be copied
        target_file = Path(self.target_dir) / "test.txt"
        self._wait_for_condition(lambda: target_file.exists(), "File copy")

        # Verify content
        with open(target_file) as f:
            self.assertEqual(f.read(), "initial content")

        # Modify the file
        with open(test_file, "w") as f:
            f.write("modified content")
        logger.info("Modified test file")

        # Wait for modification to be copied
        self._wait_for_condition(
            lambda: target_file.exists() and target_file.read_text() == "modified content",
            "File modification"
        )

        # Delete the file
        test_file.unlink()
        logger.info("Deleted test file")

        # Wait for file to be deleted in target
        self._wait_for_condition(lambda: not target_file.exists(), "File deletion")

    def test_directory_operations(self):
        """Test creating and deleting directories with nested files"""
        # Create directory structure
        source_dir = Path(self.source_dir) / "test_dir" / "nested_dir"
        source_dir.mkdir(parents=True)
        test_file = source_dir / "test.txt"
        with open(test_file, "w") as f:
            f.write("nested file content")
        logger.info(f"Created nested directory structure: {source_dir}")

        # Wait for directory and file to be copied
        target_dir = Path(self.target_dir) / "test_dir" / "nested_dir"
        target_file = target_dir / "test.txt"
        self._wait_for_condition(lambda: target_file.exists(), "Directory creation")

        # Verify content
        with open(target_file) as f:
            self.assertEqual(f.read(), "nested file content")

        # Delete the directory
        shutil.rmtree(source_dir.parent)
        logger.info("Deleted test directory")

        # Wait for directory to be deleted in target
        self._wait_for_condition(lambda: not target_dir.parent.exists(), "Directory deletion")

    def test_rapid_file_creation(self):
        """Test creating many files rapidly"""
        # Create multiple files quickly
        test_files = []
        for i in range(50):
            test_file = Path(self.source_dir) / f"test_{i}.txt"
            with open(test_file, "w") as f:
                f.write(f"content {i}")
            test_files.append(test_file)
        logger.info(f"Created {len(test_files)} test files")

        # Wait for all files to be copied
        target_files = [Path(self.target_dir) / f"test_{i}.txt" for i in range(50)]
        self._wait_for_condition(
            lambda: all(f.exists() for f in target_files),
            "Multiple file creation"
        )

        # Verify content of each file
        for i, target_file in enumerate(target_files):
            with open(target_file) as f:
                self.assertEqual(f.read(), f"content {i}")

    def _wait_for_condition(self, condition, operation, timeout=10, check_interval=0.1):
        """Helper method to wait for a condition with timeout"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if condition():
                logger.info(f"{operation} completed successfully")
                return True
            time.sleep(check_interval)
        self.fail(f"Timeout waiting for {operation}")


if __name__ == '__main__':
    unittest.main()