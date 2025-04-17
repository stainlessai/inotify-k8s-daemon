FROM python:3.8-slim-buster

# Install required packages
RUN pip install --no-cache-dir watchdog lockfile

# Copy the watcher script
COPY file_watcher.py /app/file_watcher.py
COPY main.py /app/main.py
COPY synchronizer.py /app/synchronizer.py
RUN chmod +x /app/main.py

# Set working directory
WORKDIR /app

# Run the watcher script
ENTRYPOINT ["python3", "/app/main.py"]
CMD ["/watch/source_root", "/watch/target_root"]
