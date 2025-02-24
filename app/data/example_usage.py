#!/usr/bin/env python3
"""Example usage of ONSDataManager class."""

import logging
from aws_manager import ONSDataManager

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting ONS Data Manager example")
    
    # Initialize the manager
    logger.debug("Creating ONSDataManager instance")
    ons_manager = ONSDataManager()
    
    try:
        # List available files
        logger.info("\n1. Listing available files:")
        files = ons_manager.list_available_files()
        
        if not files:
            logger.warning("No files found in the bucket!")
        else:
            logger.info(f"Found {len(files)} files. Showing first 5:")
            for file in files[:5]:
                logger.info(f"- {file['key']} (Size: {file['size']} bytes, Modified: {file['last_modified']})")
        
        if files:
            # Download and analyze the first file
            first_file = files[0]['key']
            logger.info(f"\n2. Analyzing file structure of: {first_file}")
            analysis = ons_manager.analyze_file_structure(first_file)
            
            logger.info("\nColumns:")
            for col in analysis['columns']:
                logger.info(f"- {col} ({analysis['dtypes'][col]})")
            
            logger.info(f"\nTotal rows: {analysis['row_count']}")
            logger.info(f"Memory usage: {analysis['memory_usage'] / 1024 / 1024:.2f} MB")
            
            # Check for updates
            logger.info("\n3. Checking for updates:")
            updates = ons_manager.check_for_updates()
            if updates:
                logger.info(f"Found {len(updates)} new or updated files:")
                for update in updates:
                    logger.info(f"- {update['key']}")
            else:
                logger.info("No new updates found")
            
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
    finally:
        # Clear old cache files
        logger.info("\n4. Clearing cache older than 24 hours:")
        from datetime import timedelta
        ons_manager.clear_cache(older_than=timedelta(days=1))
        logger.info("Cache cleared successfully")

if __name__ == "__main__":
    main() 