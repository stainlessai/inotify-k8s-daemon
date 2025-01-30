#!/usr/bin/env python3

import sys
import os
import time
import shutil
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
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


class FileHandler(FileSystemEventHandler):
    def __init__(self, source_dir, target_dir, timeout=1):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.timeout = timeout
        self.pending_files = {}
        self.running = True

    def on_created(self, event):
        filepath = Path(event.src_path)
        logger.info(f"Created: {filepath}")

        if event.is_directory:
            try:
                relative_path = filepath.relative_to(self.source_dir)
                target_path = self.target_dir / relative_path
                target_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {target_path}")
            except (IOError, OSError) as e:
                logger.error(f"Error creating directory {filepath}: {str(e)}")
            except ValueError as e:
                logger.error(f"Path error with {filepath}: {str(e)}")
        else:
            self.pending_files[filepath] = time.time()

    def on_modified(self, event):
        if event.is_directory:
            return

        filepath = Path(event.src_path)
        logger.info(f"File modified: {filepath}")
        self.pending_files[filepath] = time.time()

    def on_moved(self, event):
        src_path = Path(event.src_path)
        dest_path = Path(event.dest_path)

        try:
            src_relative = src_path.relative_to(self.source_dir)
            dest_relative = dest_path.relative_to(self.source_dir)

            src_target = self.target_dir / src_relative
            dest_target = self.target_dir / dest_relative

            if src_target.exists():
                if event.is_directory:
                    shutil.move(str(src_target), str(dest_target))
                    logger.info(f"Moved directory from {src_target} to {dest_target}")
                else:
                    shutil.move(str(src_target), str(dest_target))
                    logger.info(f"Moved file from {src_target} to {dest_target}")
        except (IOError, OSError) as e:
            logger.error(f"Error moving {src_path} to {dest_path}: {str(e)}")
        except ValueError as e:
            logger.error(f"Path error with move operation: {str(e)}")

    def on_deleted(self, event):
        filepath = Path(event.src_path)
        try:
            relative_path = filepath.relative_to(self.source_dir)
            target_path = self.target_dir / relative_path

            if target_path.exists():
                if event.is_directory:
                    shutil.rmtree(target_path)
                    logger.info(f"Removed directory: {target_path}")
                else:
                    target_path.unlink()
                    logger.info(f"Removed file: {target_path}")

                if filepath in self.pending_files:
                    del self.pending_files[filepath]
        except (IOError, OSError) as e:
            logger.error(f"Error removing {filepath}: {str(e)}")
        except ValueError as e:
            logger.error(f"Path error with {filepath}: {str(e)}")

    def process_pending_files(self):
        current_time = time.time()
        files_to_remove = []

        for filepath, start_time in self.pending_files.items():
            if current_time - start_time > self.timeout:
                try:
                    if filepath.exists():  # Check if file still exists
                        relative_path = filepath.relative_to(self.source_dir)
                        target_path = self.target_dir / relative_path

                        # Create parent directories if they don't exist
                        target_path.parent.mkdir(parents=True, exist_ok=True)

                        # Copy the file with metadata
                        shutil.copy2(filepath, target_path)
                        logger.info(f"Successfully copied {filepath} to {target_path}")
                        files_to_remove.append(filepath)
                except (IOError, OSError) as e:
                    logger.error(f"Error copying {filepath}: {str(e)}")
                except ValueError as e:
                    logger.error(f"Path error with {filepath}: {str(e)}")

        for filepath in files_to_remove:
            del self.pending_files[filepath]

    def stop(self):
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
        raise ValueError(f"Subpath '{subpath}' attempts to escape base directory")

    if not full_path.exists():
        if create_dirs:
            logger.info(f"Creating directory: {full_path} ...")
            full_path.mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError(f"Path {full_path} does not exist")
    elif not full_path.is_dir():
        raise ValueError(f"Path {full_path} exists but is not a directory")

    return str(full_path)


def run_watcher(source_dir, target_dir, recursive=True):
    """Main function to run the file watcher"""
    try:
        # Create the event handler and observer
        handler = FileHandler(source_dir, target_dir)
        observer = Observer()
        observer.schedule(handler, source_dir, recursive=recursive)
        observer.start()

        logger.info(f"Starting to watch directory: {source_dir}")
        logger.info(f"Target directory: {target_dir}")
        logger.info(f"Recursive mode: {recursive}")

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Shutting down...")
            handler.stop()
            observer.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Main loop
        while handler.running:
            handler.process_pending_files()
            time.sleep(0.3)

        observer.join()
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

    logger.debug(f"base_source_dir: {base_source_dir}")
    logger.debug(f"base_target_dir: {base_target_dir}")

    # Get subpaths from environment variables
    source_subpath = os.getenv('SOURCE_SUBPATH', '')
    target_subpath = os.getenv('TARGET_SUBPATH', '')
    recursive = os.getenv('RECURSIVE', 'true').lower() == 'true'

    try:
        create_subpaths_source = os.getenv('CREATE_SUBPATHS_SOURCE', 'false').lower() == 'true'
        create_subpaths_target = os.getenv('CREATE_SUBPATHS_TARGET', 'false').lower() == 'true'

        # Validate and create full paths
        source_dir = validate_and_create_path(base_source_dir, source_subpath, create_dirs=create_subpaths_source)
        target_dir = validate_and_create_path(base_target_dir, target_subpath, create_dirs=create_subpaths_target)

        logger.info(f"Full source path: {source_dir}")
        logger.info(f"Full target path: {target_dir}")

        # Run the watcher
        run_watcher(source_dir, target_dir, recursive)

    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()