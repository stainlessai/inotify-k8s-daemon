#!/usr/bin/env python3

import sys
import os
import time
import shutil
import logging
from pathlib import Path
import pyinotify
import signal

# Configure logging
logging.basicConfig(
    level=os.getenv('DEBUG_LEVEL', 'INFO'),
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
        self.running = True

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

    def stop(self):
        """Stop the file handler"""
        self.running = False


def validate_and_create_path(base_dir, subpath, create_dirs=False):
    """Validate and create full path from base directory and subpath"""
    logger.debug("entering validate_and_create_path")
    logger.debug(f"base_dir: {base_dir}")
    logger.debug(f"subpath: {subpath}")
    logger.debug(f"create_dirs: {create_dirs}")

    full_path = Path(f"{base_dir}/{subpath}")

    # Convert to absolute path and check if it's still under base_dir
    full_path = full_path.resolve()
    base_path = Path(base_dir).resolve()

    logger.debug(f"full_path: {full_path}")
    logger.debug(f"base_path: {base_path}")

    if not str(full_path).startswith(str(base_path)):
        raise ValueError(f"Subpath '{subpath}' attempts to escape base directory in '{full_path}'")

    # Create directory if it doesn't exist
    if not full_path.exists():
        if create_dirs:
            logger.info("Creating directory: " + str(full_path) + " ...")
            full_path.mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError(f"Path {full_path} does not exist")
    elif not full_path.is_dir():
        raise ValueError(f"Path {full_path} exists but is not a directory")

    return str(full_path)


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

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Shutting down...")
            handler.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Process events until stopped
        while handler.running:
            notifier.process_events()
            if notifier.check_events():
                notifier.read_events()

            # Check pending files periodically
            handler.check_pending_files()

            # Small sleep to prevent high CPU usage
            time.sleep(0.3)

        logger.info("File watcher shutting down...")

    except Exception as e:
        logger.error(f"Error in watcher: {str(e)}")
        sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print("Usage: script.py <source_directory> <target_directory>")
        sys.exit(1)

    base_source_dir = sys.argv[1]
    base_target_dir = sys.argv[2]

    logger.debug("base_source_dir: " + base_source_dir)
    logger.debug("base_target_dir: " + base_target_dir)

    # Get subpaths from environment variables
    source_subpath = os.getenv('SOURCE_SUBPATH', '')
    target_subpath = os.getenv('TARGET_SUBPATH', '')

    try:
        create_subpaths_source = os.getenv('CREATE_SUBPATHS_SOURCE', 'false').lower() == 'true'
        create_subpaths_target = os.getenv('CREATE_SUBPATHS_TARGET', 'false').lower() == 'true'

        # Validate and create full paths
        source_dir = validate_and_create_path(base_source_dir,
                                              source_subpath,
                                              create_dirs=create_subpaths_source)
        target_dir = validate_and_create_path(base_target_dir,
                                              target_subpath,
                                              create_dirs=create_subpaths_target)

        logger.info(f"Full source path: {source_dir}")
        logger.info(f"Full target path: {target_dir}")

        # Run the watcher
        run_watcher(source_dir, target_dir)

    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()