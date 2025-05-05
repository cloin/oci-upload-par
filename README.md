# OCI Directory Uploader

A Python utility for uploading directories to Oracle Cloud Infrastructure using PAR (Pre-Authenticated Request) links. 

## Features

- Upload entire directories to OCI Object Storage using PAR links
- Multipart upload support for large files
- Progress reporting for each file
- Concurrent uploads to improve performance
- Dry run mode to preview what would be uploaded
- Prefix support to organize files in the cloud storage

## Requirements

- Python 3.6+
- Required packages (see `requirements.txt`)

## Installation

1. Clone this repository
2. Install dependencies:

```
pip install -r requirements.txt
```

## Usage

Basic usage:

```
python oci_uploader.py /path/to/directory --par-url "https://objectstorage.region.oraclecloud.com/p/..." --prefix "data/"
```

### Command Line Arguments

| Argument | Description |
|----------|-------------|
| `directory` | Directory containing files to upload |
| `--par-url` | Pre-Authenticated Request URL for uploads (required) |
| `--prefix` | Prefix to add to object names (e.g., 'data/') |
| `--dry-run` | Simulate the upload without actually transferring files |
| `--no-recursive` | Do not recursively scan directories |
| `--max-workers` | Maximum number of concurrent uploads (default: 5) |
| `--chunk-size` | Chunk size for multipart uploads in bytes (default: 10MB) |
| `--verbose` | Enable verbose logging |

### Examples

Upload a directory with a specific prefix:

```
python oci_uploader.py ~/big/data/dir --par-url "https://objectstorage.region.oraclecloud.com/p/..." --prefix "data/"
```

Perform a dry run to see what would be uploaded:

```
python oci_uploader.py ~/big/data/dir --par-url "https://objectstorage.region.oraclecloud.com/p/..." --prefix "data/" --dry-run
```

Increase concurrent uploads for better performance:

```
python oci_uploader.py ~/big/data/dir --par-url "https://objectstorage.region.oraclecloud.com/p/..." --prefix "data/" --max-workers 10
```

## Notes

- PAR links must have the appropriate permissions for uploads
- For large directories, consider increasing the max workers parameter
- The dry run option is recommended before performing large uploads
