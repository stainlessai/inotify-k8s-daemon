# inotify-k8s-daemon

A small daemon deployment designed to live-copy files from a source directory to a target
directory.

## Installation

Configure your volumes and mount them as `/watch/source` and `/watch/target`

```
# Default volumes and mounts
volumes:
  - name: source-dir
    persistentVolumeClaim:
        name: my-source-volume
  - name: target-dir
    persistentVolumeClaim:
        name: my-target-volume

volumeMounts:
  - name: source-dir
    mountPath: /watch/source
  - name: target-dir
    mountPath: /watch/target
```

Any files that are closed for writing in the source directory will be immediatly copied
to the target directory
