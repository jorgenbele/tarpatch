# tarpatch

This small rust application makes it possible to do delta updates of tar files.

## Usage

### Create patch
```bash
$ tarpatch diff old.tar new.tar patch.tar
```

### Apply patch
```bash
$ tarpatch apply outdated.tar patch.tar updated.tar
```
