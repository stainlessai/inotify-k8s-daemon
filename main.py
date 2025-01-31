import logging
import os
import signal
import sys
import time
import traceback
from pathlib import Path

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from file_watcher import FileHandler
from synchronizer import Synchronizer

# Configure logging
logging.basicConfig(
    level=os.getenv('DEBUG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_watcher.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('Main')

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
    """Main function to run the file watcher and synchronizer"""
    synchronizer = None

    try:
        # Start synchronizer if enabled
        if os.getenv('SYNCHRONIZE', 'false').lower() == 'true':
            max_workers = int(os.getenv('SYNC_MAX_WORKERS', '4'))
            sync_interval = int(os.getenv('SYNC_INTERVAL', '300'))  # 5 minutes default
            logger.info(f"Starting background synchronizer (interval: {sync_interval}s)")
            synchronizer = Synchronizer(source_dir, target_dir,
                                        max_workers=max_workers,
                                        sync_interval=sync_interval)
            synchronizer.start_once()

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
            if synchronizer:
                synchronizer.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Main loop
        while handler.running:
            handler.process_pending_files()
            time.sleep(0.3)

        observer.join()

        # Clean up synchronizer if it was started
        if synchronizer:
            synchronizer.stop()

        logger.info("Services shut down successfully")

    except Exception as e:
        logger.error(f"Error in services: {str(e)}")
        if synchronizer:
            synchronizer.stop()
        sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print("Usage: main.py <source_directory> <target_directory>")
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