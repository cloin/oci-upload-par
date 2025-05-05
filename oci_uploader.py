#!/usr/bin/env python3
"""
OCI Directory Uploader

A tool to upload directories to Oracle Cloud Infrastructure using PAR links.
Supports multipart uploads, dry runs, and progress reporting.
"""

import argparse
import logging
import mimetypes
import os
import re
import sys
import threading
import time
import urllib.parse
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class OCIUploader:
    """Class to handle uploading files to OCI using PAR links."""

    def __init__(self, par_url, max_workers=5, chunk_size=10 * 1024 * 1024):
        """
        Initialize the uploader with PAR URL and configuration settings.
        
        Args:
            par_url (str): Pre-Authenticated Request URL for uploads
            max_workers (int): Maximum number of concurrent uploads
            chunk_size (int): Size of chunks for multipart uploads in bytes
        """
        self.par_url = par_url
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.parsed_url = self._parse_par_url(par_url)
        
        # Create an OCI Object Storage client configured to use the PAR
        self.client = self._create_client()
        
    def _parse_par_url(self, par_url):
        """
        Parse the PAR URL to extract components needed for the API.
        
        Args:
            par_url (str): Pre-Authenticated Request URL
            
        Returns:
            dict: Components of the PAR URL
        """
        parsed = urllib.parse.urlparse(par_url)
        
        # Extract endpoint from the hostname
        endpoint = f"{parsed.scheme}://{parsed.netloc}"
        
        # Extract namespace, bucket and PAR ID from the path
        path_parts = parsed.path.strip('/').split('/')
        
        # The typical PAR URL format is:
        # https://<endpoint>/p/<par_id>/n/<namespace>/b/<bucket>/o/<object_name_prefix>
        
        par_info = {
            'endpoint': endpoint,
            'path': parsed.path,
            'query': parsed.query
        }
        
        # Extract PAR ID if present
        par_id_index = path_parts.index('p') if 'p' in path_parts else -1
        if par_id_index != -1 and len(path_parts) > par_id_index + 1:
            par_info['par_id'] = path_parts[par_id_index + 1]
        
        return par_info
        
    def _create_client(self):
        """
        With PAR URLs, we don't actually need an OCI client.
        This method is kept for compatibility but returns None.
        
        Returns:
            None: We'll use direct HTTP requests instead
        """
        # For PAR URLs, we'll use direct requests instead of the OCI client
        # as PAR URLs are pre-authenticated and don't require the OCI SDK
        return None
    
    def _get_content_type(self, file_path):
        """
        Determine the content type of a file.
        
        Args:
            file_path (Path): Path to the file
            
        Returns:
            str: Content type of the file
        """
        content_type, _ = mimetypes.guess_type(str(file_path))
        return content_type or 'application/octet-stream'
    
    def _get_object_name(self, file_path, source_dir, prefix):
        """
        Determine the object name for a file based on source directory and prefix.
        
        Args:
            file_path (Path): Path to the file
            source_dir (Path): Base directory for scanning
            prefix (str): Prefix to prepend to the object name
            
        Returns:
            str: Object name for the file in OCI
        """
        # Get the relative path from the source directory
        rel_path = file_path.relative_to(source_dir)
        
        # Combine prefix with relative path to get the object name
        prefix = prefix.rstrip('/')
        object_name = f"{prefix}/{str(rel_path)}" if prefix else str(rel_path)
        
        # Normalize object name to use forward slashes
        object_name = object_name.replace('\\', '/')
        
        return object_name
    
    def upload_file(self, file_path, object_name, dry_run=False):
        """
        Upload a single file to OCI Object Storage using the PAR URL.
        
        Args:
            file_path (Path): Path to the file to upload
            object_name (str): Name of the object in OCI
            dry_run (bool): If True, only simulate the upload
            
        Returns:
            bool: True if upload was successful or simulated, False otherwise
        """
        file_size = file_path.stat().st_size
        
        # Determine whether to use multipart upload
        use_multipart = file_size > self.chunk_size
        
        logger.info(f"{'[DRY RUN] Would upload' if dry_run else 'Uploading'} {file_path} to {object_name} ({self._format_size(file_size)})")
        
        if dry_run:
            # Simulate successful upload for dry runs
            return True
        
        try:
            if use_multipart:
                return self._upload_file_multipart(file_path, object_name, file_size)
            else:
                return self._upload_file_single(file_path, object_name)
        except Exception as e:
            logger.error(f"Error uploading {file_path}: {str(e)}")
            return False
    
    def _upload_file_single(self, file_path, object_name):
        """
        Upload a file in a single PUT operation.
        
        Args:
            file_path (Path): Path to the file to upload
            object_name (str): Name of the object in OCI
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            # Formulate the upload URL by appending the object name to the PAR URL
            upload_url = self._get_upload_url(object_name)
            
            content_type = self._get_content_type(file_path)
            
            # Create a PUT request to the upload URL
            headers = {
                'Content-Type': content_type
            }
            
            with open(file_path, 'rb') as f:
                response = requests.put(upload_url, data=f, headers=headers)
            
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error in single upload for {file_path}: {str(e)}")
            return False
    
    def _upload_file_multipart(self, file_path, object_name, file_size):
        """
        Upload a file using multipart upload.
        
        Args:
            file_path (Path): Path to the file to upload
            object_name (str): Name of the object in OCI
            file_size (int): Size of the file in bytes
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            # For multipart uploads, we need to create a new upload URL for each part
            upload_url = self._get_upload_url(object_name)
            
            # Calculate the number of parts
            part_count = (file_size + self.chunk_size - 1) // self.chunk_size
            
            # Create a progress bar
            progress_bar = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Uploading {file_path.name}")
            
            # Read and upload each part
            with open(file_path, 'rb') as f:
                for part_num in range(1, part_count + 1):
                    # Calculate chunk size for current part
                    current_chunk_size = min(self.chunk_size, file_size - f.tell())
                    
                    # Upload the chunk
                    part_url = f"{upload_url}?partNum={part_num}"
                    headers = {
                        'Content-Type': 'application/octet-stream'
                    }
                    
                    # Create a chunk reader to read only the needed amount of data
                    chunk_data = f.read(current_chunk_size)
                    
                    # Upload using requests
                    response = requests.put(part_url, data=chunk_data, headers=headers)
                    
                    # Update progress
                    progress_bar.update(len(chunk_data))
                    
                    if response.status_code != 200:
                        logger.error(f"Error uploading part {part_num} for {file_path}: {response.text}")
                        progress_bar.close()
                        return False
            
            progress_bar.close()
            return True
        except Exception as e:
            logger.error(f"Error in multipart upload for {file_path}: {str(e)}")
            return False
    
    def _get_upload_url(self, object_name):
        """
        Create the full upload URL for an object.
        
        Args:
            object_name (str): Name of the object in OCI
            
        Returns:
            str: Full URL for uploading the object
        """
        # Encode the object name
        encoded_object = urllib.parse.quote(object_name)
        
        # Append the object name to the PAR URL
        # The exact format depends on how the PAR URL is structured
        if self.parsed_url['path'].endswith('/o/'):
            # PAR URL already has the object prefix
            upload_url = f"{self.par_url}{encoded_object}"
        else:
            # PAR URL needs the object path added
            upload_url = f"{self.par_url}/o/{encoded_object}"
        
        return upload_url
    
    def _format_size(self, size_bytes):
        """
        Format file size in a human-readable format.
        
        Args:
            size_bytes (int): Size in bytes
            
        Returns:
            str: Formatted size string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"


def scan_directory(directory, prefix="", recursive=True):
    """
    Scan a directory for files to upload.
    
    Args:
        directory (str or Path): Directory to scan
        prefix (str): Prefix to prepend to object names
        recursive (bool): Whether to scan subdirectories recursively
        
    Returns:
        list: List of Path objects for files found
    """
    directory = Path(directory).expanduser().resolve()
    
    if not directory.exists() or not directory.is_dir():
        logger.error(f"Directory does not exist or is not a directory: {directory}")
        return []
    
    files = []
    
    # Create a glob pattern based on recursion setting
    pattern = "**/*" if recursive else "*"
    
    # Scan for files
    for file_path in directory.glob(pattern):
        if file_path.is_file():
            files.append(file_path)
    
    logger.info(f"Found {len(files)} files in {directory}")
    return files


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Upload directories to OCI using PAR links")
    
    parser.add_argument("directory", help="Directory containing files to upload")
    parser.add_argument("--par-url", required=True, help="Pre-Authenticated Request URL for uploads")
    parser.add_argument("--prefix", default="", help="Prefix to add to object names (e.g., 'data/')")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the upload without actually transferring files")
    parser.add_argument("--no-recursive", action="store_true", help="Do not recursively scan directories")
    parser.add_argument("--max-workers", type=int, default=5, help="Maximum number of concurrent uploads")
    parser.add_argument("--chunk-size", type=int, default=10 * 1024 * 1024, help="Chunk size for multipart uploads in bytes")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set logging level based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Print configuration
    logger.info(f"Directory: {args.directory}")
    logger.info(f"PAR URL: {args.par_url}")
    logger.info(f"Prefix: {args.prefix}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Recursive: {not args.no_recursive}")
    logger.info(f"Max workers: {args.max_workers}")
    logger.info(f"Chunk size: {args.chunk_size} bytes")
    
    # Scan the directory for files
    files = scan_directory(args.directory, args.prefix, not args.no_recursive)
    
    if not files:
        logger.warning("No files found to upload.")
        return
    
    # Calculate total size
    total_size = sum(file_path.stat().st_size for file_path in files)
    logger.info(f"Total upload size: {total_size / (1024 * 1024):.2f} MB")
    
    # Create an uploader
    uploader = OCIUploader(args.par_url, args.max_workers, args.chunk_size)
    
    # Start uploading files
    successful_uploads = 0
    failed_uploads = 0
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Create a list of futures for uploading each file
        futures = []
        source_dir = Path(args.directory).expanduser().resolve()
        
        for file_path in files:
            # Determine the object name
            object_name = uploader._get_object_name(file_path, source_dir, args.prefix)
            
            # Schedule the upload
            future = executor.submit(uploader.upload_file, file_path, object_name, args.dry_run)
            futures.append((future, file_path, object_name))
        
        # Process the results as they complete
        for future, file_path, object_name in futures:
            try:
                result = future.result()
                if result:
                    successful_uploads += 1
                    logger.debug(f"Successfully uploaded {file_path} to {object_name}")
                else:
                    failed_uploads += 1
                    logger.error(f"Failed to upload {file_path} to {object_name}")
            except Exception as e:
                failed_uploads += 1
                logger.error(f"Exception while uploading {file_path}: {str(e)}")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Print summary
    logger.info("Upload summary:")
    logger.info(f"  {'Would have uploaded' if args.dry_run else 'Uploaded'} {successful_uploads} files successfully")
    logger.info(f"  Failed to upload {failed_uploads} files")
    logger.info(f"  Total time: {elapsed_time:.2f} seconds")
    
    if args.dry_run:
        logger.info("Dry run completed. No files were actually uploaded.")
    
    # Exit with appropriate status code
    sys.exit(0 if failed_uploads == 0 else 1)


if __name__ == "__main__":
    main()
