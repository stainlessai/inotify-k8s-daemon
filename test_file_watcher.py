#!/usr/bin/env python3
import unittest
import tempfile
import shutil
import os
import time
import logging
from pathlib import Path
from unittest.mock import Mock, patch, PropertyMock
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileDeletedEvent, DirCreatedEvent
from watchdog.observers import Observer

from file_watcher import FileHandler

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestFileHandler(unittest.TestCase):
    def setUp(self):
        # Create temporary directories for testing
        self.source_dir = tempfile.mkdtemp()
        self.target_dir = tempfile.mkdtemp()
        self.handler = FileHandler(self.source_dir, self.target_dir)
        # Give the event processing thread time to start
        time.sleep(0.1)

    def tearDown(self):
        # Stop the handler and its threads
        self.handler.stop()
        # Clean up temporary directories
        shutil.rmtree(self.source_dir)
        shutil.rmtree(self.target_dir)

    def wait_for_file_processing(self, filepath, timeout=5):
        """Helper method to wait for a file to be processed"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.handler.pending_files_lock:
                if filepath not in self.handler.pending_files:
                    return True
            time.sleep(0.1)
            self.handler.process_pending_files()
        return False

    def test_file_created_event(self):
        # Create a test file
        test_file = Path(self.source_dir) / "test.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        # Simulate file creation event
        event = FileCreatedEvent(str(test_file))
        self.handler.on_created(event)

        # Wait for event queue processing
        time.sleep(1)

        # Verify file is in pending_files
        with self.handler.pending_files_lock:
            self.assertIn(test_file, self.handler.pending_files)

        # Process pending files and wait for completion
        self.assertTrue(self.wait_for_file_processing(test_file))

        # Check if file was copied to target
        target_file = Path(self.target_dir) / "test.txt"
        self.assertTrue(target_file.exists())
        with open(target_file) as f:
            self.assertEqual(f.read(), "test content")

    def test_directory_created_event(self):
        # Create a test directory path
        test_dir = Path(self.source_dir) / "test_dir"

        # Simulate directory creation event
        event = DirCreatedEvent(str(test_dir))
        self.handler.on_created(event)

        # Wait for event processing
        time.sleep(1)

        # Check if directory was created in target
        target_dir = Path(self.target_dir) / "test_dir"
        self.assertTrue(target_dir.exists())
        self.assertTrue(target_dir.is_dir())

    def test_file_modified_event(self):
        # Create a test file
        test_file = Path(self.source_dir) / "test.txt"
        with open(test_file, "w") as f:
            f.write("initial content")

        # Simulate file modification
        with open(test_file, "w") as f:
            f.write("modified content")

        event = FileModifiedEvent(str(test_file))
        self.handler.on_modified(event)

        # Wait for event queue processing
        time.sleep(1)

        # Verify file is in pending_files
        with self.handler.pending_files_lock:
            self.assertIn(test_file, self.handler.pending_files)

        # Process pending files and wait for completion
        self.assertTrue(self.wait_for_file_processing(test_file))

        # Check if modified file was copied to target
        target_file = Path(self.target_dir) / "test.txt"
        self.assertTrue(target_file.exists())
        with open(target_file) as f:
            self.assertEqual(f.read(), "modified content")

    def test_file_deleted_event(self):
        # Create and copy a test file
        test_file = Path(self.source_dir) / "test.txt"
        target_file = Path(self.target_dir) / "test.txt"

        with open(test_file, "w") as f:
            f.write("test content")
        shutil.copy(test_file, target_file)

        # Add file to pending_files
        with self.handler.pending_files_lock:
            self.handler.pending_files[test_file] = (time.time(), 0)

        # Simulate file deletion
        event = FileDeletedEvent(str(test_file))
        self.handler.on_deleted(event)

        # Wait for the event to be processed by the queue
        time.sleep(1)

        # Verify file is removed from pending_files
        with self.handler.pending_files_lock:
            retry_count = 0
            while test_file in self.handler.pending_files and retry_count < 5:
                time.sleep(0.2)
                retry_count += 1
            self.assertNotIn(test_file, self.handler.pending_files)

        # Check if file was deleted from target
        self.assertFalse(target_file.exists())

    def test_retry_mechanism(self):
        # Create a test file
        test_file = Path(self.source_dir) / "test.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        # Mock shutil.copyfile to fail a few times
        original_copyfile = shutil.copyfile
        fail_count = [0]

        def mock_copyfile(*args, **kwargs):
            fail_count[0] += 1
            if fail_count[0] <= 2:  # Fail first two attempts
                raise IOError("Mock copy failure")
            return original_copyfile(*args, **kwargs)

        with patch('shutil.copyfile', side_effect=mock_copyfile):
            # Simulate file creation
            event = FileCreatedEvent(str(test_file))
            self.handler.on_created(event)

            # Wait for event queue processing
            time.sleep(1)

            # Process pending files multiple times
            for _ in range(5):
                self.handler.process_pending_files()
                time.sleep(1)

            # Verify file was eventually copied
            target_file = Path(self.target_dir) / "test.txt"
            self.assertTrue(target_file.exists())
            with open(target_file) as f:
                self.assertEqual(f.read(), "test content")

    def test_rapid_event_handling(self):
        """Test that rapid-fire events are not lost"""
        # Create a set of test files rapidly
        test_files = set()
        events_sent = 0

        # Send a burst of create events for the same file
        test_file = Path(self.source_dir) / "test.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        # Rapidly send multiple events
        for _ in range(100):
            events_sent += 1
            event = FileCreatedEvent(str(test_file))
            self.handler.on_created(event)
            event = FileModifiedEvent(str(test_file))
            self.handler.on_modified(event)
            test_files.add(test_file)

        # Wait for event processing
        time.sleep(2)

        # Verify the file is in pending_files
        with self.handler.pending_files_lock:
            self.assertIn(test_file, self.handler.pending_files)

        # Process the files
        self.assertTrue(self.wait_for_file_processing(test_file))

        # Verify file was copied
        target_file = Path(self.target_dir) / "test.txt"
        self.assertTrue(target_file.exists())

        # Verify event queue is empty
        self.assertTrue(self.handler.event_queue.empty())

        # Check queue statistics
        logger.info(f"Events sent: {events_sent}")
        logger.info(f"Queue size: {self.handler.event_queue.qsize()}")


class TestMainFunctionality(unittest.TestCase):
    def setUp(self):
        self.source_dir = tempfile.mkdtemp()
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.source_dir)
        shutil.rmtree(self.target_dir)

    def test_validate_and_create_path(self):
        from main import validate_and_create_path

        # Test valid path creation
        test_subpath = "test_subdir"
        full_path = validate_and_create_path(self.source_dir, test_subpath, create_dirs=True)
        self.assertTrue(Path(full_path).exists())
        self.assertTrue(Path(full_path).is_dir())

        # Test path traversal attempt
        with self.assertRaises(ValueError):
            validate_and_create_path(self.source_dir, "../outside", create_dirs=True)

        # Test non-existent path without create_dirs
        with self.assertRaises(ValueError):
            validate_and_create_path(self.source_dir, "nonexistent", create_dirs=False)


if __name__ == '__main__':
    unittest.main()