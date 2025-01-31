#!/usr/bin/env python3
import concurrent
import sys
import os
import time
import shutil
import logging
import traceback
from pathlib import Path
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor
import threading
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
    def __init__(self, source_dir, target_dir, timeout=1, max_workers=4):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.timeout = timeout
        self.pending_files = {}  # {filepath: (start_time, retry_count)}
        self.running = True
        self.retry_limit = int(os.getenv('RETRY_LIMIT', '10'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '5'))
        logger.info(f"Retry limit: {self.retry_limit}")
        logger.info(f"Max workers: {self.max_workers}")
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.pending_files_lock = threading.Lock()

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
            self.pending_files[filepath] = (time.time(), 0)  # Already using tuple format

    def on_modified(self, event):
        if event.is_directory:
            return

        filepath = Path(event.src_path)
        logger.info(f"File modified: {filepath}")
        self.pending_files[filepath] = (time.time(), 0)  # Fix: Use tuple instead of float

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

    def process_single_file(self, filepath, start_time, retry_count):
        """Process a single file and return whether it should be removed from pending"""
        thread_id = threading.get_ident()
        logger.debug(f"[Thread-{thread_id}] Starting to process file: {filepath}")
        logger.debug(f"[Thread-{thread_id}] Current retry count: {retry_count}")

        try:
            if not filepath.exists():
                logger.warning(f"[Thread-{thread_id}] File {filepath} no longer exists")
                return True

            relative_path = filepath.relative_to(self.source_dir)
            target_path = self.target_dir / relative_path

            logger.debug(f"[Thread-{thread_id}] Calculated target path: {target_path}")
            logger.debug(f"[Thread-{thread_id}] Creating parent directories if needed: {target_path.parent}")

            # Create parent directories if they don't exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Log file details before copy
            source_size = filepath.stat().st_size
            logger.debug(f"[Thread-{thread_id}] Attempting to copy file: {filepath} ({source_size} bytes)")
            copy_start_time = time.time()

            # Copy the file without metadata
            # FIXME make configurable (some filesystems don't allow metadata copy)
            shutil.copyfile(filepath, target_path)

            copy_duration = time.time() - copy_start_time
            target_size = target_path.stat().st_size
            logger.info(f"[Thread-{thread_id}] Successfully copied {filepath} to {target_path}")
            logger.debug(f"[Thread-{thread_id}] Copy completed in {copy_duration:.2f}s, size: {target_size} bytes")
            return True

        except (IOError, OSError) as e:
            retry_count += 1
            error_details = traceback.format_exc()
            logger.debug(f"[Thread-{thread_id}] Full error traceback:\n{error_details}")

            if retry_count >= self.retry_limit:
                logger.error(f"[Thread-{thread_id}] Failed to copy {filepath} after {retry_count} attempts: {str(e)}")
                return True
            else:
                logger.warning(f"[Thread-{thread_id}] Retry {retry_count}/{self.retry_limit} for {filepath}: {str(e)}")
                with self.pending_files_lock:
                    logger.debug(f"[Thread-{thread_id}] Updating pending files with new retry count")
                    self.pending_files[filepath] = (time.time(), retry_count)
                return False

        except ValueError as e:
            logger.error(f"[Thread-{thread_id}] Path error with {filepath}: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Path error details:\n{traceback.format_exc()}")
            return True
        except Exception as e:
            logger.error(f"[Thread-{thread_id}] Unexpected error processing {filepath}: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Unexpected error details:\n{traceback.format_exc()}")
            return True

    def process_pending_files(self):
        current_time = time.time()
        files_to_process = []

        # Gather files that are ready to process
        with self.pending_files_lock:
            # logger.debug(f"Current pending files count: {len(self.pending_files)}")
            for filepath, (start_time, retry_count) in self.pending_files.items():
                wait_time = current_time - start_time
                logger.debug(f"File {filepath} has been waiting for {wait_time:.2f}s (timeout: {self.timeout}s)")
                if wait_time > self.timeout:
                    files_to_process.append((filepath, start_time, retry_count))

        if not files_to_process:
            # logger.debug("No files ready for processing")
            return

        logger.info(f"Submitting {len(files_to_process)} files for parallel processing")
        logger.debug(f"Files to process: {[str(f[0]) for f in files_to_process]}")

        # Submit all files to thread pool
        future_to_file = {
            self.executor.submit(self.process_single_file, filepath, start_time, retry_count): filepath
            for filepath, start_time, retry_count in files_to_process
        }

        # Process completed futures
        files_to_remove = set()
        for future in concurrent.futures.as_completed(future_to_file):
            filepath = future_to_file[future]
            logger.debug(f"Processing completed future for file: {filepath}")
            try:
                should_remove = future.result()
                if should_remove:
                    logger.debug(f"Marking file for removal: {filepath}")
                    files_to_remove.add(filepath)
            except Exception as e:
                logger.error(f"Unexpected error processing {filepath}: {str(e)}")
                logger.debug(f"Unexpected error details:\n{traceback.format_exc()}")
                files_to_remove.add(filepath)

        # Remove processed files
        with self.pending_files_lock:
            logger.debug(f"Removing {len(files_to_remove)} processed files from pending list")
            for filepath in files_to_remove:
                if filepath in self.pending_files:
                    del self.pending_files[filepath]
            logger.debug(f"Remaining pending files: {len(self.pending_files)}")

    def stop(self):
        self.running = False
        self.executor.shutdown(wait=True)  # Wait for all pending tasks to complete


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

        if os.getenv('POLLING_OBSERVER', 'False').lower() == 'true':
            observer = PollingObserver()
        else:
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