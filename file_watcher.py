#!/usr/bin/env python3
import concurrent
import os
import time
import shutil
import logging
import traceback
from pathlib import Path
from queue import Queue, Empty

from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor
import threading

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
        self.delete_before_update = os.getenv('DELETE_BEFORE_UPDATE', 'True').lower() == 'true'
        logger.info(f"Retry limit: {self.retry_limit}")
        logger.info(f"Max workers: {self.max_workers}")
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.pending_files_lock = threading.Lock()
        # Add an event queue for incoming events
        self.event_queue = Queue()
        # Start event processing thread
        self.event_thread = threading.Thread(target=self._process_event_queue, daemon=True)
        self.event_thread.start()

    def _process_event_queue(self):
        """Process events from the queue in a dedicated thread"""
        while self.running:
            try:
                event_type, filepath = self.event_queue.get(timeout=1)
                with self.pending_files_lock:
                    if event_type == 'created' or event_type == 'modified':
                        self.pending_files[filepath] = (time.time(), 0)
                    elif event_type == 'deleted':
                        if filepath in self.pending_files:
                            del self.pending_files[filepath]
                self.event_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                traceback.print_exc()

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
            # Queue the event instead of processing directly
            self.event_queue.put(('created', filepath))

    def on_modified(self, event):
        if event.is_directory:
            return

        filepath = Path(event.src_path)
        logger.info(f"File modified: {filepath}")
        self.event_queue.put(('modified', filepath))

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

                self.event_queue.put(('deleted', filepath))
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

            if target_path.exists():
                if self.delete_before_update:
                    logger.debug(f"[Thread-{thread_id}] Deleting {target_path} since it already "
                                 f"exists and DELETE_BEFORE_UPDATE is set to True")
                    os.remove(target_path)
                else:
                    logger.warning(f"[Thread-{thread_id}] File {target_path} already exists")

            # Log file details before copy
            source_size = filepath.stat().st_size
            logger.debug(f"[Thread-{thread_id}] Attempting to copy file: {filepath} ({source_size} bytes)")
            copy_start_time = time.time()

            # Copy the file without metadata
            # FIXME make configurable (some filesystems don't allow metadata copy)
            shutil.copyfile(filepath, target_path)

            copy_duration = time.time() - copy_start_time
            target_size = target_path.stat().st_size

            if source_size != target_size:
                logger.warning(f"[Thread-{thread_id}] File size mismatch: source_size={source_size} "
                               f"!= target_size={target_size} for {filepath}")
            else:
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
        self.event_thread.join()
        self.executor.shutdown(wait=True)
