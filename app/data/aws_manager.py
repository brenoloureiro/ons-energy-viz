#!/usr/bin/env python3
"""
ONS Data Manager for AWS S3 access.
This module provides functionality to interact with ONS public data stored in AWS S3.
"""

import os
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Configure logging with formato mais detalhado
logging.basicConfig(
    level=logging.DEBUG,  # Mudando para DEBUG para ver mais detalhes
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ONSDataManager:
    """
    Manages access to ONS data stored in AWS S3 public bucket.
    
    This class handles connection to the ONS public bucket, file listing,
    downloading, caching, and data analysis operations.
    """
    
    def __init__(
        self,
        bucket_name: str = 'ons-aws-prod-opendata',
        base_path: str = 'dataset/geracao_usina_2_ho/',
        cache_dir: str = './cache',
        cache_ttl: int = 3600  # 1 hour in seconds
    ):
        """
        Initialize ONS Data Manager.
        
        Args:
            bucket_name (str): Name of the S3 bucket
            base_path (str): Base path in the bucket to look for files
            cache_dir (str): Local directory for caching files
            cache_ttl (int): Time to live for cache in seconds
        """
        logger.info(f"Initializing ONSDataManager with bucket: {bucket_name}, base_path: {base_path}")
        self.bucket_name = bucket_name
        self.base_path = base_path
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = cache_ttl
        self.s3_client = None
        self.last_update_check = None
        
        # Create cache directory if it doesn't exist
        logger.debug(f"Creating cache directory at: {self.cache_dir}")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize connection
        self._initialize_connection()

    def _initialize_connection(self) -> None:
        """
        Initialize connection to AWS S3.
        No credentials needed for public bucket access.
        """
        try:
            logger.debug("Creating S3 client...")
            self.s3_client = boto3.client('s3')
            logger.info("Successfully initialized S3 client")
            
            # Testar a conexão listando o bucket
            logger.debug(f"Testing connection by listing bucket: {self.bucket_name}")
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def list_available_files(self, refresh: bool = False) -> List[Dict]:
        """
        List available files in the specified S3 path.
        
        Args:
            refresh (bool): Force refresh the file list
            
        Returns:
            List[Dict]: List of file information dictionaries
        """
        try:
            logger.debug(f"Listing files with prefix: {self.base_path}")
            if not refresh and self.last_update_check and \
               (datetime.now() - self.last_update_check).seconds < self.cache_ttl:
                logger.debug("Using cached file list")
                return self._read_file_list_cache()

            logger.debug("Making API call to list objects")
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.base_path
            )
            
            logger.debug(f"Response received: {response}")
            
            files = []
            if 'Contents' in response:
                logger.info(f"Found {len(response['Contents'])} objects in bucket")
                for obj in response['Contents']:
                    if obj['Key'].endswith(('.parquet', '.csv')):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
                        logger.debug(f"Added file to list: {obj['Key']}")
            else:
                logger.warning(f"No contents found in bucket {self.bucket_name} with prefix {self.base_path}")
            
            self._save_file_list_cache(files)
            self.last_update_check = datetime.now()
            
            logger.info(f"Found {len(files)} matching files in bucket")
            return files
            
        except ClientError as e:
            logger.error(f"Error listing files: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing files: {str(e)}")
            raise

    def download_file(self, file_key: str, force_download: bool = False) -> Path:
        """
        Download a specific file from S3 and cache it locally.
        
        Args:
            file_key (str): S3 key of the file to download
            force_download (bool): Force download even if cached
            
        Returns:
            Path: Path to the local cached file
        """
        local_path = self.cache_dir / Path(file_key).name
        
        # Check if file exists in cache and is not expired
        if not force_download and local_path.exists():
            if (datetime.now() - datetime.fromtimestamp(local_path.stat().st_mtime)).seconds < self.cache_ttl:
                logger.info(f"Using cached file: {local_path}")
                return local_path

        try:
            logger.info(f"Downloading file: {file_key}")
            self.s3_client.download_file(
                self.bucket_name,
                file_key,
                str(local_path)
            )
            return local_path
            
        except ClientError as e:
            logger.error(f"Error downloading file {file_key}: {str(e)}")
            raise

    def read_file(self, file_key: str) -> pd.DataFrame:
        """
        Read a file from cache or download it if necessary.
        
        Args:
            file_key (str): S3 key of the file to read
            
        Returns:
            pd.DataFrame: DataFrame containing the file contents
        """
        local_path = self.download_file(file_key)
        
        try:
            if str(local_path).endswith('.parquet'):
                df = pd.read_parquet(local_path)
            else:  # CSV
                df = pd.read_csv(local_path)
            
            logger.info(f"Successfully read file: {file_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading file {file_key}: {str(e)}")
            raise

    def analyze_file_structure(self, file_key: str) -> Dict:
        """
        Analyze the structure of a specific file.
        
        Args:
            file_key (str): S3 key of the file to analyze
            
        Returns:
            Dict: Dictionary containing file structure information
        """
        df = self.read_file(file_key)
        
        analysis = {
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'row_count': len(df),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'null_counts': df.isnull().sum().to_dict(),
            'sample_rows': df.head().to_dict() if len(df) > 0 else {}
        }
        
        logger.info(f"Completed structure analysis for {file_key}")
        return analysis

    def check_for_updates(self) -> List[Dict]:
        """
        Check for new or updated files in the bucket.
        
        Returns:
            List[Dict]: List of new or updated files
        """
        current_files = self.list_available_files(refresh=True)
        cached_files = self._read_file_list_cache()
        
        updates = []
        for file in current_files:
            cached_file = next(
                (f for f in cached_files if f['key'] == file['key']),
                None
            )
            
            if not cached_file or \
               file['last_modified'] > cached_file['last_modified']:
                updates.append(file)
        
        if updates:
            logger.info(f"Found {len(updates)} new or updated files")
        
        return updates

    def _read_file_list_cache(self) -> List[Dict]:
        """Read the cached file list."""
        cache_file = self.cache_dir / 'file_list_cache.parquet'
        if cache_file.exists():
            return pd.read_parquet(cache_file).to_dict('records')
        return []

    def _save_file_list_cache(self, files: List[Dict]) -> None:
        """Save the file list to cache."""
        cache_file = self.cache_dir / 'file_list_cache.parquet'
        pd.DataFrame(files).to_parquet(cache_file)

    def clear_cache(self, older_than: Optional[timedelta] = None) -> None:
        """
        Clear the local cache.
        
        Args:
            older_than (timedelta, optional): Only clear files older than this
        """
        try:
            for file in self.cache_dir.glob('*'):
                if file.is_file():
                    if older_than is None or \
                       (datetime.now() - datetime.fromtimestamp(file.stat().st_mtime)) > older_than:
                        file.unlink()
                        logger.info(f"Removed cached file: {file}")
                        
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
            raise 