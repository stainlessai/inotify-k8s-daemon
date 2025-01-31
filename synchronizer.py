import concurrent.futures
import threading
import queue
import time
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
        files_found = 0

        try:
            for source_file in self.source_dir.rglob('*'):
                if not self.running:
                    break

                if source_file.is_file():
                    rel_path = source_file.relative_to(self.source_dir)
                    target_file = self.target_dir / rel_path

                    if not target_file.exists():
                        logger.debug(f"Found missing file: {rel_path}")
                        self.file_queue.put(source_file)
                        files_found += 1

            logger.info(f"Directory scan complete. Found {files_found} files to synchronize")

        except Exception as e:
            logger.error(f"Error scanning directory: {str(e)}")
        finally:
            # Put sentinel values for each worker to signal completion
            for _ in range(self.max_workers):
                self.file_queue.put(None)

    def copy_file_with_retry(self, source_file, retry_count=0):
        """Copy a single file with retry mechanism"""
        if not self.running:
            return False

        try:
            relative_path = source_file.relative_to(self.source_dir)
            target_path = self.target_dir / relative_path

            logger.debug(f"Attempting to copy: {source_file} -> {target_path}")

            # Create parent directories if they don't exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy with metadata
            shutil.copyfile(source_file, target_path)
            logger.info(f"Successfully copied: {relative_path}")
            return True

        except (IOError, OSError) as e:
            retry_count += 1
            if retry_count >= self.retry_limit:
                logger.error(f"Failed to copy {source_file} after {retry_count} attempts: {str(e)}")
                return False
            else:
                logger.warning(f"Retry {retry_count}/{self.retry_limit} for {source_file}: {str(e)}")
                time.sleep(1)  # Add a small delay before retry
                return self.copy_file_with_retry(source_file, retry_count)

        except Exception as e:
            logger.error(f"Unexpected error copying {source_file}: {str(e)}")
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