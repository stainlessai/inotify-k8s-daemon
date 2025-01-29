FROM python:3.11-slim

# Install required packages
RUN pip install --no-cache-dir pyinotify python-daemon lockfile

# Copy the watcher script
COPY file_watcher.py /app/file_watcher.py
RUN chmod +x /app/file_watcher.py

# Set working directory
WORKDIR /app

# Run the watcher script
ENTRYPOINT ["/usr/local/bin/python3", "/app/file_watcher.py"]
CMD ["/watch/source", "/watch/target"]