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
            [pd.Grouper(key='DATA', freq=freq)] + (group_by if group_by else [])
        ).agg({
            'GERACAO': ['sum', 'mean', 'std'],
            'COMBUSTIVEL': 'first'
        }).reset_index()
        
        return df_agg
        
    def calculate_statistics(
        self,
        group_by: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Calculate descriptive statistics for the data.
        
        Args:
            group_by (List[str], optional): Columns to group by for statistics
            
        Returns:
            pd.DataFrame: DataFrame with statistics
        """
        logger.info("Calculating descriptive statistics")
        
        df = self._load_and_preprocess_data()
        
        stats_df = df.groupby(group_by if group_by else ['COMBUSTIVEL']).agg({
            'GERACAO': [
                'count', 'mean', 'std', 'min', 'max',
                lambda x: x.quantile(0.25),
                lambda x: x.quantile(0.75)
            ]
        })
        
        # Rename stat columns
        stats_df.columns = [
            'count', 'mean', 'std', 'min', 'max', 'q25', 'q75'
        ]
        
        return stats_df
        
    def normalize_data(
        self,
        method: str = 'minmax',
        by_group: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Normalize generation data for comparison.
        
        Args:
            method (str): Normalization method ('minmax', 'zscore', or 'robust')
            by_group (List[str], optional): Columns to group by for normalization
            
        Returns:
            pd.DataFrame: DataFrame with normalized values
        """
        logger.info(f"Normalizing data using {method} method")
        
        df = self._load_and_preprocess_data()
        
        if by_group:
            groups = df.groupby(by_group)
        else:
            groups = [(None, df)]
            
        normalized_dfs = []
        for name, group in groups:
            if method == 'minmax':
                normalized = (group['GERACAO'] - group['GERACAO'].min()) / \
                           (group['GERACAO'].max() - group['GERACAO'].min())
            elif method == 'zscore':
                normalized = (group['GERACAO'] - group['GERACAO'].mean()) / group['GERACAO'].std()
            elif method == 'robust':
                median = group['GERACAO'].median()
                iqr = group['GERACAO'].quantile(0.75) - group['GERACAO'].quantile(0.25)
                normalized = (group['GERACAO'] - median) / iqr
            else:
                raise ValueError(f"Unknown normalization method: {method}")
                
            group = group.copy()
            group['GERACAO_NORMALIZED'] = normalized
            normalized_dfs.append(group)
            
        return pd.concat(normalized_dfs)
        
    def analyze_trends(
        self,
        freq: str = 'M',
        decompose_method: str = 'additive'
    ) -> Dict[str, pd.Series]:
        """
        Analyze trends and seasonality in the data.
        
        Args:
            freq (str): Frequency for analysis
            decompose_method (str): Decomposition method ('additive' or 'multiplicative')
            
        Returns:
            Dict[str, pd.Series]: Dictionary with trend, seasonal, and residual components
        """
        logger.info("Analyzing trends and seasonality")
        
        # Aggregate data to desired frequency
        df_agg = self.aggregate_by_time(freq=freq)
        
        # Perform seasonal decomposition
        decomposition = seasonal_decompose(
            df_agg['GERACAO']['sum'],
            period=12 if freq == 'M' else 52 if freq == 'W' else 7,
            model=decompose_method
        )
        
        return {
            'trend': decomposition.trend,
            'seasonal': decomposition.seasonal,
            'residual': decomposition.resid
        }
        
    def prepare_visualization_data(
        self,
        viz_type: str,
        **kwargs
    ) -> Union[pd.DataFrame, Dict]:
        """
        Prepare data for specific visualization types.
        
        Args:
            viz_type (str): Type of visualization ('timeseries', 'pie', 'bar', 'map')
            **kwargs: Additional arguments specific to each visualization type
            
        Returns:
            Union[pd.DataFrame, Dict]: Prepared data in appropriate format
        """
        logger.info(f"Preparing data for {viz_type} visualization")
        
        if viz_type == 'timeseries':
            return self._prepare_timeseries_data(**kwargs)
        elif viz_type == 'pie':
            return self._prepare_pie_data(**kwargs)
        elif viz_type == 'bar':
            return self._prepare_bar_data(**kwargs)
        elif viz_type == 'map':
            return self._prepare_map_data(**kwargs)
        else:
            raise ValueError(f"Unknown visualization type: {viz_type}")
            
    def _prepare_timeseries_data(
        self,
        freq: str = 'D',
        group_by: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Prepare data for time series visualization."""
        return self.aggregate_by_time(freq=freq, group_by=group_by)
        
    def _prepare_pie_data(
        self,
        group_by: List[str] = ['COMBUSTIVEL'],
        **kwargs
    ) -> pd.DataFrame:
        """Prepare data for pie chart visualization."""
        df = self._load_and_preprocess_data()
        return df.groupby(group_by)['GERACAO'].sum().reset_index()
        
    def _prepare_bar_data(
        self,
        group_by: List[str] = ['COMBUSTIVEL'],
        sort_by: str = 'GERACAO',
        ascending: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """Prepare data for bar chart visualization."""
        df = self._load_and_preprocess_data()
        return df.groupby(group_by)['GERACAO'].sum() \
               .reset_index() \
               .sort_values(sort_by, ascending=ascending)
               
    def _prepare_map_data(
        self,
        region_column: str = 'SUBSISTEMA',
        **kwargs
    ) -> Dict:
        """Prepare data for map visualization."""
        df = self._load_and_preprocess_data()
        return df.groupby(region_column)['GERACAO'].sum().to_dict()
        
    def export_data(
        self,
        data: pd.DataFrame,
        format: str = 'csv',
        output_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Export processed data to various formats.
        
        Args:
            data (pd.DataFrame): Data to export
            format (str): Export format ('csv', 'json', 'excel', 'parquet')
            output_path (str, optional): Path to save the exported file
            
        Returns:
            Optional[str]: Path to the exported file if output_path is provided
        """
        logger.info(f"Exporting data to {format} format")
        
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
        if format == 'csv':
            if output_path:
                data.to_csv(output_path, index=False)
                return str(output_path)
            return data.to_csv(index=False)
        elif format == 'json':
            if output_path:
                data.to_json(output_path, orient='records')
                return str(output_path)
            return data.to_json(orient='records')
        elif format == 'excel':
            if not output_path:
                raise ValueError("output_path is required for Excel format")
            data.to_excel(output_path, index=False)
            return str(output_path)
        elif format == 'parquet':
            if not output_path:
                raise ValueError("output_path is required for Parquet format")
            data.to_parquet(output_path, index=False)
            return str(output_path)
        else:
            raise ValueError(f"Unknown export format: {format}") 