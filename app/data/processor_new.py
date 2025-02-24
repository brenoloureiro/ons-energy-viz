#!/usr/bin/env python3
"""
Data Processor for ONS energy generation data.
This module provides functionality to process, transform and analyze energy generation data.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple
import json

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.tsa.seasonal import seasonal_decompose

from .aws_manager import ONSDataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Processes and transforms ONS energy generation data for visualization and analysis.
    
    This class handles data processing, aggregation, statistical analysis,
    and preparation of data for various visualization formats.
    """
    
    def __init__(
        self,
        ons_manager: ONSDataManager,
        cache_dir: str = './cache/processed',
        cache_ttl: int = 3600 * 24  # 24 hours in seconds
    ):
        """
        Initialize Data Processor.
        
        Args:
            ons_manager (ONSDataManager): Instance of ONSDataManager for data access
            cache_dir (str): Directory for caching processed data
            cache_ttl (int): Time to live for cache in seconds
        """
        self.ons_manager = ons_manager
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = cache_ttl
        self.data_cache = {}
        
        # Create cache directory if it doesn't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("DataProcessor initialized")
        
    def _load_and_preprocess_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        force_reload: bool = False
    ) -> pd.DataFrame:
        """
        Load and preprocess raw data from ONS.
        
        Args:
            start_date (datetime, optional): Start date for filtering data
            end_date (datetime, optional): End date for filtering data
            force_reload (bool): Force reload from source instead of cache
            
        Returns:
            pd.DataFrame: Preprocessed DataFrame
        """
        cache_key = f"raw_data_{start_date}_{end_date}"
        
        if not force_reload and cache_key in self.data_cache:
            logger.debug("Using cached raw data")
            return self.data_cache[cache_key]
            
        logger.info("Loading and preprocessing raw data")
        
        # Get list of available files
        files = self.ons_manager.list_available_files()
        
        # Filter files by date range if specified
        if start_date or end_date:
            files = [
                f for f in files
                if self._is_file_in_date_range(f['key'], start_date, end_date)
            ]
        
        # Load and concatenate all relevant files
        dfs = []
        for file in files:
            df = self.ons_manager.read_file(file['key'])
            dfs.append(df)
            
        if not dfs:
            raise ValueError("No data found for the specified period")
            
        # Concatenate all DataFrames
        df = pd.concat(dfs, ignore_index=True)
        
        # Basic preprocessing
        df = self._clean_data(df)
        
        # Cache the result
        self.data_cache[cache_key] = df
        
        return df
        
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize raw data.
        
        Args:
            df (pd.DataFrame): Raw DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.debug("Cleaning data")
        
        # Convert date columns to datetime
        date_columns = df.filter(like='DATA').columns
        for col in date_columns:
            df[col] = pd.to_datetime(df[col])
            
        # Handle missing values
        df = df.fillna({
            'GERACAO': 0,  # Assume 0 generation for missing values
            'COMBUSTIVEL': 'OUTROS'  # Categorize unknown fuel types
        })
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Sort by date
        df = df.sort_values(by=date_columns[0])
        
        return df
        
    def _is_file_in_date_range(
        self,
        file_key: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> bool:
        """Check if file contains data within the specified date range."""
        # Extract date from filename
        # Assuming format like GERACAO_USINA-2_YYYY.parquet or GERACAO_USINA-2_YYYY_MM.parquet
        parts = file_key.split('_')
        year = int(parts[-1].split('.')[0])
        month = 1
        
        if len(parts[-1].split('.')[0]) > 4:  # If monthly file
            month = int(parts[-1].split('.')[0].split('_')[1])
            
        file_date = datetime(year, month, 1)
        
        if start_date and file_date < start_date:
            return False
        if end_date and file_date > end_date:
            return False
            
        return True
        
    def aggregate_by_time(
        self,
        freq: str = 'M',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        group_by: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aggregate data by time frequency.
        
        Args:
            freq (str): Frequency string ('D' for daily, 'W' for weekly, 'M' for monthly, 'Y' for yearly)
            start_date (datetime, optional): Start date for filtering
            end_date (datetime, optional): End date for filtering
            group_by (List[str], optional): Additional columns to group by
            
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        logger.info(f"Aggregating data by {freq} frequency")
        
        df = self._load_and_preprocess_data(start_date, end_date)
        
        # Prepare grouping columns
        group_cols = ['DATA']
        if group_by:
            group_cols.extend(group_by)
            
        # Resample and aggregate
        df_agg = df.groupby(
            [pd.Grouper(key='DATA', freq=freq)], as_index=False
        ).agg({
            'GERACAO': 'sum',
            'COMBUSTIVEL': 'first'
        })
        
        # Sort by date
        df_agg = df_agg.sort_values(by='DATA')
        
        return df_agg 