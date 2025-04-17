import concurrent.futures
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import logging
import os
import traceback

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
        self.delete_queue = queue.Queue()  # New queue for orphaned files to delete
        self.running = True
        self.background_thread = None
        self.worker_threads = []

        # Set to True to delete orphaned files (files in target but not in source)
        self.cleanup_orphaned = os.getenv('CLEANUP_ORPHANED', 'True').lower() == 'true'

        # Add stats tracking
        self.stats = {
            'files_scanned': 0,
            'files_queued': 0,
            'files_copied': 0,
            'copy_failures': 0,
            'orphaned_files_found': 0,
            'orphaned_files_deleted': 0,
            'orphaned_delete_failures': 0,
            'total_bytes_copied': 0,
            'scan_start_time': None,
            'last_progress_time': 0
        }
        self.stats_lock = threading.Lock()

        logger.info(f"Initialized Synchronizer with max_workers={max_workers}, sync_interval={sync_interval}s")
        logger.info(f"Cleanup orphaned files: {self.cleanup_orphaned}")
        logger.debug(f"Source directory: {source_dir}")
        logger.debug(f"Target directory: {target_dir}")

    def log_progress(self, force=False):
        """Log progress stats if a minute has passed or if forced"""
        current_time = time.time()
        with self.stats_lock:
            if force or (current_time - self.stats['last_progress_time']) >= 60:
                elapsed = current_time - self.stats['scan_start_time'] if self.stats['scan_start_time'] else 0
                logger.info("Sync Progress Report:")
                logger.info(f"- Time elapsed: {elapsed:.1f} seconds")
                logger.info(f"- Files scanned: {self.stats['files_scanned']}")
                logger.info(f"- Files queued for copy: {self.stats['files_queued']}")
                logger.info(f"- Files successfully copied: {self.stats['files_copied']}")
                logger.info(f"- Copy failures: {self.stats['copy_failures']}")
                logger.info(f"- Orphaned files found: {self.stats['orphaned_files_found']}")
                logger.info(f"- Orphaned files deleted: {self.stats['orphaned_files_deleted']}")
                logger.info(f"- Orphaned delete failures: {self.stats['orphaned_delete_failures']}")
                logger.info(f"- Total data copied: {self.stats['total_bytes_copied'] / (1024 * 1024):.1f} MB")
                if self.stats['files_copied'] > 0:
                    success_rate = (self.stats['files_copied'] /
                                    (self.stats['files_copied'] + self.stats['copy_failures'])) * 100
                    logger.info(f"- Copy success rate: {success_rate:.1f}%")
                if self.stats['orphaned_files_found'] > 0:
                    delete_success_rate = (self.stats['orphaned_files_deleted'] /
                                           self.stats['orphaned_files_found']) * 100
                    logger.info(f"- Delete success rate: {delete_success_rate:.1f}%")
                self.stats['last_progress_time'] = current_time

    def find_missing_and_orphaned_files(self):
        """Find missing files in source and orphaned files in target"""
        logger.info("Starting directory scan - files will be processed in reverse alphabetical order")
        files_found = 0
        orphaned_files_found = 0

        with self.stats_lock:
            self.stats['scan_start_time'] = time.time()
            self.stats['files_scanned'] = 0
            self.stats['files_queued'] = 0
            self.stats['orphaned_files_found'] = 0

        try:
            # Build a set of all files in the source directory (relative paths)
            source_files = set()

            # Step 1: First scan source directory and build the file set
            for current_dir, subdirs, files in os.walk(str(self.source_dir)):
                if not self.running:
                    break

                # Sort files in reverse order
                files.sort(reverse=True)

                # Sort subdirectories in reverse order so they're processed in reverse alpha
                subdirs.sort(reverse=True)

                current_path = Path(current_dir)
                for filename in files:
                    if not self.running:
                        break

                    source_file = current_path / filename
                    with self.stats_lock:
                        self.stats['files_scanned'] += 1

                    try:
                        rel_path = source_file.relative_to(self.source_dir)
                        target_file = self.target_dir / rel_path

                        # Add to source files set
                        source_files.add(str(rel_path))

                        # Check if target file exists
                        if source_file.is_file() and not target_file.exists():
                            logger.debug(f"Found missing file: {rel_path}")
                            self.file_queue.put(source_file)
                            with self.stats_lock:
                                self.stats['files_queued'] += 1
                                files_found += 1
                    except (OSError, ValueError) as e:
                        logger.warning(f"Error processing file {source_file}: {e}")
                        continue

                    # Log progress every minute
                    self.log_progress()

            # Step 2: Only if cleanup_orphaned is enabled, scan target directory
            if self.cleanup_orphaned:
                logger.info("Starting scan for orphaned files in target directory")

                for current_dir, subdirs, files in os.walk(str(self.target_dir)):
                    if not self.running:
                        break

                    # Sort files in reverse order
                    files.sort(reverse=True)

                    # Sort subdirectories in reverse order so they're processed in reverse alpha
                    subdirs.sort(reverse=True)

                    current_path = Path(current_dir)
                    for filename in files:
                        if not self.running:
                            break

                        target_file = current_path / filename

                        try:
                            rel_path = target_file.relative_to(self.target_dir)

                            # If this file doesn't exist in source, it's orphaned
                            if str(rel_path) not in source_files:
                                logger.debug(f"Found orphaned file: {rel_path}")
                                self.delete_queue.put(target_file)
                                with self.stats_lock:
                                    self.stats['orphaned_files_found'] += 1
                                    orphaned_files_found += 1
                        except (OSError, ValueError) as e:
                            logger.warning(f"Error processing target file {target_file}: {e}")
                            continue

                        # Log progress every minute
                        self.log_progress()

            logger.info(
                f"Directory scan complete. Found {files_found} files to synchronize and {orphaned_files_found} orphaned files to clean up")

        except Exception as e:
            logger.error(f"Error scanning directory: {str(e)}")
            logger.debug(f"Stack trace:\n{traceback.format_exc()}")
        finally:
            # Put sentinel values for each worker to signal completion
            for _ in range(self.max_workers):
                self.file_queue.put(None)
                if self.cleanup_orphaned:
                    self.delete_queue.put(None)

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

            # Copy the file
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

            with self.stats_lock:
                self.stats['files_copied'] += 1
                self.stats['total_bytes_copied'] += source_size

            logger.info(f"[Thread-{thread_id}] Successfully copied: {relative_path}")
            logger.debug(f"[Thread-{thread_id}] Copy completed in {copy_duration:.2f}s")
            logger.debug(f"[Thread-{thread_id}] Final size: {target_size} bytes")

            return True

        except (IOError, OSError) as e:
            retry_count += 1
            logger.debug(f"[Thread-{thread_id}] Copy failed with error: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Full error traceback:\n{traceback.format_exc()}")

            if retry_count >= self.retry_limit:
                logger.error(
                    f"[Thread-{thread_id}] Failed to copy {source_file} after {retry_count} attempts: {str(e)}")
                with self.stats_lock:
                    self.stats['copy_failures'] += 1
                return False
            else:
                logger.warning(
                    f"[Thread-{thread_id}] Retry {retry_count}/{self.retry_limit} for {source_file}: {str(e)}")
                time.sleep(1)  # Add a small delay before retry
                return self.copy_file_with_retry(source_file, retry_count)

        except Exception as e:
            logger.error(f"[Thread-{thread_id}] Unexpected error copying {source_file}: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Stack trace:\n{traceback.format_exc()}")
            with self.stats_lock:
                self.stats['copy_failures'] += 1
            return False

    def delete_orphaned_file_with_retry(self, file_path, retry_count=0):
        """Delete an orphaned file with retry mechanism"""
        if not self.running:
            return False

        thread_id = threading.get_ident()
        logger.debug(f"[Thread-{thread_id}] Starting to delete orphaned file: {file_path}")
        logger.debug(f"[Thread-{thread_id}] Current retry count: {retry_count}")

        try:
            if not file_path.exists():
                logger.warning(f"[Thread-{thread_id}] Orphaned file {file_path} no longer exists")
                return True

            # Get relative path for logging
            relative_path = file_path.relative_to(self.target_dir)

            # Delete the file
            file_path.unlink()

            with self.stats_lock:
                self.stats['orphaned_files_deleted'] += 1

            logger.info(f"[Thread-{thread_id}] Successfully deleted orphaned file: {relative_path}")
            return True

        except (IOError, OSError) as e:
            retry_count += 1
            logger.debug(f"[Thread-{thread_id}] Delete failed with error: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Full error traceback:\n{traceback.format_exc()}")

            if retry_count >= self.retry_limit:
                logger.error(
                    f"[Thread-{thread_id}] Failed to delete orphaned file {file_path} after {retry_count} attempts: {str(e)}")
                with self.stats_lock:
                    self.stats['orphaned_delete_failures'] += 1
                return False
            else:
                logger.warning(
                    f"[Thread-{thread_id}] Retry {retry_count}/{self.retry_limit} for deletion of {file_path}: {str(e)}")
                time.sleep(1)  # Add a small delay before retry
                return self.delete_orphaned_file_with_retry(file_path, retry_count)

        except Exception as e:
            logger.error(f"[Thread-{thread_id}] Unexpected error deleting orphaned file {file_path}: {str(e)}")
            logger.debug(f"[Thread-{thread_id}] Stack trace:\n{traceback.format_exc()}")
            with self.stats_lock:
                self.stats['orphaned_delete_failures'] += 1
            return False

    def copy_worker_process(self, worker_id):
        """Worker process that takes files from the queue and copies them"""
        logger.debug(f"Copy worker {worker_id} started")
        success_count = 0
        failure_count = 0

        while self.running:
            try:
                source_file = self.file_queue.get()
                if source_file is None:  # Sentinel value
                    logger.debug(f"Copy worker {worker_id} received stop signal")
                    break

                if self.copy_file_with_retry(source_file):
                    success_count += 1
                else:
                    failure_count += 1

                self.file_queue.task_done()
                self.log_progress()  # Update progress after each file

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Copy worker {worker_id} error: {str(e)}")
                logger.debug(f"Stack trace:\n{traceback.format_exc()}")
                failure_count += 1

        logger.debug(f"Copy worker {worker_id} stopped. Successes: {success_count}, Failures: {failure_count}")
        return success_count, failure_count

    def delete_worker_process(self, worker_id):
        """Worker process that takes files from the delete queue and removes them"""
        logger.debug(f"Delete worker {worker_id} started")
        success_count = 0
        failure_count = 0

        while self.running:
            try:
                target_file = self.delete_queue.get()
                if target_file is None:  # Sentinel value
                    logger.debug(f"Delete worker {worker_id} received stop signal")
                    break

                if self.delete_orphaned_file_with_retry(target_file):
                    success_count += 1
                else:
                    failure_count += 1

                self.delete_queue.task_done()
                self.log_progress()  # Update progress after each file

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Delete worker {worker_id} error: {str(e)}")
                logger.debug(f"Stack trace:\n{traceback.format_exc()}")
                failure_count += 1

        logger.debug(f"Delete worker {worker_id} stopped. Successes: {success_count}, Failures: {failure_count}")
        return success_count, failure_count

    def synchronize_once(self):
        """Perform a single directory synchronization"""
        if not self.running:
            return 0, 0, 0, 0

        sync_start_time = time.time()
        logger.info("Starting streaming synchronization with orphaned file cleanup")

        # Reset stats for this sync operation
        with self.stats_lock:
            for key in self.stats:
                if isinstance(self.stats[key], (int, float)):
                    self.stats[key] = 0

        # Start worker threads
        copy_worker_futures = []
        delete_worker_futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as worker_executor:
            # Start copy workers
            for i in range(self.max_workers):
                future = worker_executor.submit(self.copy_worker_process, i)
                copy_worker_futures.append(future)

            # Start delete workers if cleanup is enabled
            if self.cleanup_orphaned:
                for i in range(self.max_workers):
                    future = worker_executor.submit(self.delete_worker_process, i)
                    delete_worker_futures.append(future)

            # Start file discovery in a separate thread
            discovery_thread = threading.Thread(target=self.find_missing_and_orphaned_files)
            discovery_thread.daemon = True
            discovery_thread.start()

            # Wait for discovery to complete and workers to process all files
            discovery_thread.join()
            self.file_queue.join()
            if self.cleanup_orphaned:
                self.delete_queue.join()

            # Get results from copy workers
            total_copy_success = 0
            total_copy_failure = 0
            for future in concurrent.futures.as_completed(copy_worker_futures):
                success, failure = future.result()
                total_copy_success += success
                total_copy_failure += failure

            # Get results from delete workers
            total_delete_success = 0
            total_delete_failure = 0
            if self.cleanup_orphaned:
                for future in concurrent.futures.as_completed(delete_worker_futures):
                    success, failure = future.result()
                    total_delete_success += success
                    total_delete_failure += failure

        # Final progress report
        self.log_progress(force=True)

        # Report total sync time
        total_sync_time = time.time() - sync_start_time
        logger.info("Synchronization Summary:")
        logger.info(f"- Total time: {total_sync_time:.1f} seconds")
        logger.info(f"- Successful copies: {total_copy_success}")
        logger.info(f"- Failed copies: {total_copy_failure}")
        logger.info(f"- Successful orphaned file deletions: {total_delete_success}")
        logger.info(f"- Failed orphaned file deletions: {total_delete_failure}")
        if total_sync_time > 0:
            logger.info(
                f"- Average speed: {(self.stats['total_bytes_copied'] / total_sync_time / 1024 / 1024):.2f} MB/s")

        return total_copy_success, total_copy_failure, total_delete_success, total_delete_failure

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
                logger.debug(f"Stack trace:\n{traceback.format_exc()}")
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

        try:
            while True:
                self.delete_queue.get_nowait()
                self.delete_queue.task_done()
        except queue.Empty:
            pass

        for _ in range(self.max_workers):
            self.file_queue.put(None)
            self.delete_queue.put(None)

        if self.background_thread:
            self.background_thread.join(timeout=30)

        self.executor.shutdown(wait=True)
        logger.debug("Synchronizer stopped")