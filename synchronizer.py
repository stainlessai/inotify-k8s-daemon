import concurrent.futures
import threading
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
        self.sync_interval = sync_interval  # Time between syncs in seconds
        self.retry_limit = int(os.getenv('RETRY_LIMIT', '10'))
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.copy_lock = threading.Lock()
        self.running = True
        self.background_thread = None

        logger.info(f"Initialized Synchronizer with max_workers={max_workers}, sync_interval={sync_interval}s")
        logger.debug(f"Source directory: {source_dir}")
        logger.debug(f"Target directory: {target_dir}")

    def find_missing_files(self):
        """Find files that exist in source but not in target directory"""
        logger.info("Starting directory comparison")
        missing_files = []

        # Recursively get all files in source directory
        for source_file in self.source_dir.rglob('*'):
            if not self.running:
                return []

            if source_file.is_file():
                # Calculate relative path to maintain directory structure
                rel_path = source_file.relative_to(self.source_dir)
                target_file = self.target_dir / rel_path

                if not target_file.exists():
                    logger.debug(f"Found missing file: {rel_path}")
                    missing_files.append(source_file)

        logger.info(f"Found {len(missing_files)} files to synchronize")
        return missing_files

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
            shutil.copy2(source_file, target_path)
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

    def synchronize_once(self):
        """Perform a single directory synchronization"""
        if not self.running:
            return 0, 0

        logger.info("Starting directory synchronization")

        # Find files that need to be synchronized
        missing_files = self.find_missing_files()

        if not missing_files:
            logger.info("No files need synchronization")
            return 0, 0

        # Create a future for each file copy operation
        future_to_file = {
            self.executor.submit(self.copy_file_with_retry, source_file): source_file
            for source_file in missing_files
        }

        # Track results
        success_count = 0
        failure_count = 0

        # Process completed futures
        for future in concurrent.futures.as_completed(future_to_file):
            if not self.running:
                break

            source_file = future_to_file[future]
            try:
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logger.error(f"Error processing {source_file}: {str(e)}")
                failure_count += 1

        logger.info(f"Synchronization complete. Successes: {success_count}, Failures: {failure_count}")
        return success_count, failure_count

    def run_background_sync(self):
        """Run continuous background synchronization"""
        while self.running:
            try:
                self.synchronize_once()

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
        self.background_thread.daemon = True  # Thread will exit when main program exits
        self.background_thread.start()

    def stop(self):
        """Stop background synchronization and clean up"""
        logger.info("Stopping Synchronizer")
        self.running = False
        if self.background_thread:
            self.background_thread.join(timeout=30)  # Wait up to 30 seconds
        self.executor.shutdown(wait=True)
        logger.debug("Synchronizer stopped")