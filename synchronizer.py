import concurrent.futures
import threading
import queue
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import logging
import os

logger = logging.getLogger('DirectorySynchronizer')


class Synchronizer:
    def __init__(self, source_dir, target_dir, max_workers=4, sync_interval=300):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.max_workers = max_workers
        self.sync_interval = sync_interval
        self.retry_limit = int(os.getenv('RETRY_LIMIT', '10'))
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.file_queue = queue.Queue()
        self.running = True
        self.background_thread = None
        self.worker_threads = []

        logger.info(f"Initialized Synchronizer with max_workers={max_workers}, sync_interval={sync_interval}s")
        logger.debug(f"Source directory: {source_dir}")
        logger.debug(f"Target directory: {target_dir}")

    def find_missing_files(self):
        """Stream missing files to the queue as they're found"""
        logger.info("Starting directory scan")
        scan_start_time = time.time()
        files_found = 0
        files_skipped = 0
        total_size = 0

        try:
            logger.debug(f"Scanning source directory: {self.source_dir}")
            for source_file in self.source_dir.rglob('*'):
                if not self.running:
                    break

                if source_file.is_file():
                    rel_path = source_file.relative_to(self.source_dir)
                    target_file = self.target_dir / rel_path
                    file_size = source_file.stat().st_size

                    logger.debug(f"Checking file: {rel_path} ({file_size} bytes)")

                    if not target_file.exists():
                        logger.debug(f"Found missing file: {rel_path}")
                        logger.debug(f"- Size: {file_size} bytes")
                        logger.debug(f"- Modified: {time.ctime(source_file.stat().st_mtime)}")
                        self.file_queue.put(source_file)
                        files_found += 1
                        total_size += file_size
                    else:
                        source_mtime = source_file.stat().st_mtime
                        target_mtime = target_file.stat().st_mtime
                        if abs(source_mtime - target_mtime) > 2:  # Allow 2 second difference
                            logger.debug(f"File exists but timestamps differ: {rel_path}")
                            logger.debug(f"- Source mtime: {time.ctime(source_mtime)}")
                            logger.debug(f"- Target mtime: {time.ctime(target_mtime)}")
                        files_skipped += 1

            scan_duration = time.time() - scan_start_time
            logger.info(f"Directory scan complete in {scan_duration:.2f}s")
            logger.info(f"- Files found missing: {files_found}")
            logger.info(f"- Files already synced: {files_skipped}")
            logger.info(f"- Total size to sync: {total_size / (1024 * 1024):.2f} MB")

        except Exception as e:
            logger.error(f"Error scanning directory: {str(e)}")
            logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        finally:
            # Put sentinel values for each worker to signal completion
            for _ in range(self.max_workers):
                self.file_queue.put(None)

    def copy_file_with_retry(self, source_file, retry_count=0):
        """Copy a single file with retry mechanism"""
        if not self.running:
            return False

        thread_id = threading.get_ident()
        logger.debug(f"[Thread-{thread_id}] Starting to process file: {source_file}")
        logger.debug(f"[Thread-{thread_id}] Current retry count: {retry_count}")

        try:
            relative_path = source_file.relative_to(self.source_dir)
            target_path = self.target_dir / relative_path

            logger.debug(f"[Thread-{thread_id}] Calculated target path: {target_path}")
            logger.debug(f"[Thread-{thread_id}] Creating parent directories if needed: {target_path.parent}")

            # Create parent directories if they don't exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Log file details before copy
            source_size = source_file.stat().st_size
            source_mtime = source_file.stat().st_mtime
            logger.debug(f"[Thread-{thread_id}] Source file details:")
            logger.debug(f"[Thread-{thread_id}] - Size: {source_size} bytes")
            logger.debug(f"[Thread-{thread_id}] - Modified time: {time.ctime(source_mtime)}")

            # Time the copy operation
            copy_start_time = time.time()

            # Copy with metadata
            shutil.copyfile(source_file, target_path)

            copy_duration = time.time() - copy_start_time
            target_size = target_path.stat().st_size
            target_mtime = target_path.stat().st_mtime

            # Verify copy
            if source_size != target_size:
                logger.warning(f"[Thread-{thread_id}] File size mismatch:")
                logger.warning(f"[Thread-{thread_id}] - Source: {source_size} bytes")
                logger.warning(f"[Thread-{thread_id}] - Target: {target_size} bytes")
                raise IOError("File size mismatch after copy")

            if abs(source_mtime - target_mtime) > 2:  # Allow 2 second difference
                logger.warning(f"[Thread-{thread_id}] File timestamp mismatch:")
                logger.warning(f"[Thread-{thread_id}] - Source: {time.ctime(source_mtime)}")
                logger.warning(f"[Thread-{thread_id}] - Target: {time.ctime(target_mtime)}")

            logger.info(f"[Thread-{thread_id}] Successfully copied: {relative_path}")
            logger.debug(f"[Thread-{thread_id}] Copy completed in {copy_duration:.2f}s")
            logger.debug(f"[Thread-{thread_id}] Final size: {target_size} bytes")

            return True

        except (IOError, OSError) as e:
            retry_count += 1
            logger.debug(f"[Thread-{thread_id}] Copy failed with error: {str(e)}")

            if retry_count >= self.retry_limit:
                logger.error(
                    f"[Thread-{thread_id}] Failed to copy {source_file} after {retry_count} attempts: {str(e)}")
                return False
            else:
                logger.warning(
                    f"[Thread-{thread_id}] Retry {retry_count}/{self.retry_limit} for {source_file}: {str(e)}")
                time.sleep(1)  # Add a small delay before retry
                return self.copy_file_with_retry(source_file, retry_count)

        except Exception as e:
            logger.error(f"[Thread-{thread_id}] Unexpected error copying {source_file}: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Stack trace:\n{traceback.format_exc()}")
            return False

    def worker_process(self, worker_id):
        """Worker process that takes files from the queue and copies them"""
        logger.debug(f"Worker {worker_id} started")
        success_count = 0
        failure_count = 0

        while self.running:
            try:
                source_file = self.file_queue.get()
                if source_file is None:  # Sentinel value
                    logger.debug(f"Worker {worker_id} received stop signal")
                    break

                if self.copy_file_with_retry(source_file):
                    success_count += 1
                else:
                    failure_count += 1

                self.file_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {str(e)}")
                failure_count += 1

        logger.debug(f"Worker {worker_id} stopped. Successes: {success_count}, Failures: {failure_count}")
        return success_count, failure_count

    def synchronize_once(self):
        """Perform a single directory synchronization"""
        if not self.running:
            return 0, 0

        logger.info("Starting streaming synchronization")

        # Start worker threads
        self.worker_threads = []
        worker_futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as worker_executor:
            # Start workers
            for i in range(self.max_workers):
                future = worker_executor.submit(self.worker_process, i)
                worker_futures.append(future)

            # Start file discovery in a separate thread
            discovery_thread = threading.Thread(target=self.find_missing_files)
            discovery_thread.daemon = True
            discovery_thread.start()

            # Wait for discovery to complete and workers to process all files
            discovery_thread.join()
            self.file_queue.join()

            # Get results from workers
            total_success = 0
            total_failure = 0
            for future in concurrent.futures.as_completed(worker_futures):
                success, failure = future.result()
                total_success += success
                total_failure += failure

        logger.info(f"Synchronization complete. Successes: {total_success}, Failures: {total_failure}")
        return total_success, total_failure

    def run_background_sync(self, single=False):
        """Run continuous background synchronization"""
        while self.running:
            try:
                self.synchronize_once()
                if single:
                    self.running = False
                    break

                # Sleep for the specified interval
                for _ in range(self.sync_interval):
                    if not self.running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error in background sync: {str(e)}")
                if self.running:
                    time.sleep(10)  # Wait before retry on error

    def start(self):
        """Start background synchronization"""
        logger.info("Starting background synchronization")
        self.background_thread = threading.Thread(target=self.run_background_sync)
        self.background_thread.daemon = True
        self.background_thread.start()

    def start_once(self):
        """Start background synchronization one time"""
        logger.info("Starting background synchronization for single sync op")
        self.background_thread = threading.Thread(target=self.run_background_sync, kwargs={'single': True})
        self.background_thread.daemon = True
        self.background_thread.start()

    def stop(self):
        """Stop background synchronization and clean up"""
        logger.info("Stopping Synchronizer")
        self.running = False

        # Clear and send stop signals to queue
        try:
            while True:
                self.file_queue.get_nowait()
                self.file_queue.task_done()
        except queue.Empty:
            pass

        for _ in range(self.max_workers):
            self.file_queue.put(None)

        if self.background_thread:
            self.background_thread.join(timeout=30)

        self.executor.shutdown(wait=True)
        logger.debug("Synchronizer stopped")