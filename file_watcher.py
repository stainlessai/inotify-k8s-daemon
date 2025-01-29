#!/usr/bin/env python3

import sys
import os
import time
import shutil
import logging
from pathlib import Path
import pyinotify
from daemon import DaemonContext
import signal
import lockfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_watcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FileWatcher')


class FileHandler(pyinotify.ProcessEvent):
    def __init__(self, source_dir, target_dir, timeout=1):
        super().__init__()
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.timeout = timeout
        self.pending_files = {}

    def process_IN_CREATE(self, event):
        """Handle file creation events"""
        if event.dir:
            return

        filepath = Path(event.pathname)
        logger.info(f"File created: {filepath}")
        self.pending_files[filepath] = time.time()

    def process_IN_MODIFY(self, event):
        """Handle file modification events"""
        if event.dir:
            return

        filepath = Path(event.pathname)
        logger.info(f"File modified: {filepath}")
        self.pending_files[filepath] = time.time()

    def process_IN_CLOSE_WRITE(self, event):
        """Handle file close events (when writing is finished)"""
        if event.dir:
            return

        filepath = Path(event.pathname)
        if filepath in self.pending_files:
            try:
                # Wait a brief moment to ensure writing is complete
                time.sleep(0.1)

                # Copy the file to target directory
                target_path = self.target_dir / filepath.name
                shutil.copy2(filepath, target_path)
                logger.info(f"Successfully copied {filepath} to {target_path}")

                # Remove from pending files
                del self.pending_files[filepath]

            except (IOError, OSError) as e:
                logger.error(f"Error copying {filepath}: {str(e)}")

    def check_pending_files(self):
        """Check and process any files that might have been missed"""
        current_time = time.time()
        files_to_remove = []

        for filepath, start_time in self.pending_files.items():
            if current_time - start_time > self.timeout:
                try:
                    target_path = self.target_dir / filepath.name
                    shutil.copy2(filepath, target_path)
                    logger.info(f"Successfully copied pending file {filepath} to {target_path}")
                    files_to_remove.append(filepath)
                except (IOError, OSError) as e:
                    logger.error(f"Error copying pending file {filepath}: {str(e)}")

        for filepath in files_to_remove:
            del self.pending_files[filepath]


def run_watcher(source_dir, target_dir):
    """Main function to run the file watcher"""
    try:
        # Create an instance of watch manager
        wm = pyinotify.WatchManager()

        # Create an instance of file handler
        handler = FileHandler(source_dir, target_dir)

        # Create notifier
        notifier = pyinotify.Notifier(wm, handler)

        # Add watch on source directory
        mask = pyinotify.IN_CREATE | pyinotify.IN_MODIFY | pyinotify.IN_CLOSE_WRITE
        wm.add_watch(source_dir, mask, rec=False, auto_add=False)

        logger.info(f"Starting to watch directory: {source_dir}")
        logger.info(f"Target directory: {target_dir}")

        # Process events forever
        while True:
            notifier.process_events()
            if notifier.check_events():
                notifier.read_events()

            # Check pending files periodically
            handler.check_pending_files()

            # Small sleep to prevent high CPU usage
            time.sleep(0.1)

    except Exception as e:
        logger.error(f"Error in watcher: {str(e)}")
        sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print("Usage: script.py <source_directory> <target_directory>")
        sys.exit(1)

    source_dir = sys.argv[1]
    target_dir = sys.argv[2]

    # Validate directories
    if not os.path.isdir(source_dir):
        print(f"Error: Source directory {source_dir} does not exist")
        sys.exit(1)

    if not os.path.isdir(target_dir):
        print(f"Error: Target directory {target_dir} does not exist")
        sys.exit(1)

    # Create daemon context
    context = DaemonContext(
        working_directory='/',
        umask=0o002,
        pidfile=lockfile.FileLock('/var/run/file_watcher.pid'),
        signal_map={
            signal.SIGTERM: lambda signo, frame: sys.exit(0),
            signal.SIGINT: lambda signo, frame: sys.exit(0),
        }
    )

    # Start daemon
    with context:
        run_watcher(source_dir, target_dir)


if __name__ == "__main__":
    print("HALLO")
    main()