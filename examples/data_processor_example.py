#!/usr/bin/env python3
"""
Example usage of the DataProcessor class.
This script demonstrates various data processing and analysis capabilities.
"""

import logging
from datetime import datetime, timedelta
import os
from pathlib import Path

from app.data.aws_manager import ONSDataManager
from app.data.processor import DataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function demonstrating DataProcessor usage."""
    try:
        # Initialize ONSDataManager
        ons_manager = ONSDataManager()
        
        # Initialize DataProcessor
        processor = DataProcessor(
            ons_manager=ons_manager,
            cache_dir='./cache/processed'
        )
        
        # Define date range for analysis
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # Last year of data
        
        logger.info("1. Aggregating data by month")
        monthly_data = processor.aggregate_by_time(
            freq='M',
            start_date=start_date,
            end_date=end_date,
            group_by=['COMBUSTIVEL']
        )
        
        # Export monthly aggregation to Excel
        os.makedirs('output', exist_ok=True)
        processor.export_data(
            monthly_data,
            format='excel',
            output_path='output/monthly_generation.xlsx'
        )
        
        logger.info("2. Calculating statistics by fuel type")
        stats = processor.calculate_statistics(group_by=['COMBUSTIVEL'])
        processor.export_data(
            stats,
            format='csv',
            output_path='output/generation_statistics.csv'
        )
        
        logger.info("3. Analyzing trends")
        trends = processor.analyze_trends(freq='M')
        
        logger.info("4. Preparing visualization data")
        # Time series data
        timeseries_data = processor.prepare_visualization_data(
            'timeseries',
            freq='D',
            group_by=['COMBUSTIVEL']
        )
        processor.export_data(
            timeseries_data,
            format='parquet',
            output_path='output/timeseries_data.parquet'
        )
        
        # Pie chart data
        pie_data = processor.prepare_visualization_data('pie')
        processor.export_data(
            pie_data,
            format='json',
            output_path='output/fuel_distribution.json'
        )
        
        # Bar chart data
        bar_data = processor.prepare_visualization_data(
            'bar',
            sort_by='GERACAO',
            ascending=False
        )
        processor.export_data(
            bar_data,
            format='csv',
            output_path='output/generation_by_fuel.csv'
        )
        
        # Map data
        map_data = processor.prepare_visualization_data('map')
        
        logger.info("5. Normalizing data")
        normalized_data = processor.normalize_data(
            method='minmax',
            by_group=['COMBUSTIVEL']
        )
        processor.export_data(
            normalized_data,
            format='csv',
            output_path='output/normalized_generation.csv'
        )
        
        logger.info("Data processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 