#!/usr/bin/env python3
"""Data normalization pipeline."""

import argparse
import sys
from pathlib import Path
import pandas as pd
import structlog

logger = structlog.get_logger()

class NormalizePipeline:
    """Data normalization pipeline."""
    
    def __init__(self):
        """Initialize normalization pipeline."""
        self.bronze_dir = Path("data/bronze")
        self.silver_dir = Path("data/silver")
        
    def normalize_products(self):
        """Normalize product data from bronze to silver."""
        logger.info("Normalizing product data...")
        
        # Find all product files in bronze
        product_files = list(self.bronze_dir.glob("*_products.parquet"))
        
        if not product_files:
            logger.warning("No product files found in bronze layer")
            return 0
        
        all_products = []
        for file_path in product_files:
            df = pd.read_parquet(file_path)
            all_products.append(df)
        
        # Combine all products
        combined_products = pd.concat(all_products, ignore_index=True)
        
        # Save to silver layer
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.silver_dir / "products.parquet"
        combined_products.to_parquet(output_path, index=False)
        
        logger.info(f"✅ Normalized {len(combined_products)} products")
        logger.info(f"📁 Saved to: {output_path}")
        
        return len(combined_products)
    
    def normalize_reviews(self):
        """Normalize review data from bronze to silver."""
        logger.info("Normalizing review data...")
        
        # Find all review files in bronze
        review_files = list(self.bronze_dir.glob("*_reviews.parquet"))
        
        if not review_files:
            logger.warning("No review files found in bronze layer")
            return 0
        
        all_reviews = []
        for file_path in review_files:
            df = pd.read_parquet(file_path)
            all_reviews.append(df)
        
        # Combine all reviews
        combined_reviews = pd.concat(all_reviews, ignore_index=True)
        
        # Save to silver layer
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.silver_dir / "reviews.parquet"
        combined_reviews.to_parquet(output_path, index=False)
        
        logger.info(f"✅ Normalized {len(combined_reviews)} reviews")
        logger.info(f"📁 Saved to: {output_path}")
        
        return len(combined_reviews)
    
    def run(self):
        """Run the normalization pipeline."""
        logger.info("🚀 Starting data normalization...")
        
        products_count = self.normalize_products()
        reviews_count = self.normalize_reviews()
        
        logger.info(f"✅ Normalization completed: {products_count} products, {reviews_count} reviews")
        
        return products_count + reviews_count

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Luxury Beauty Data Normalization Pipeline")
    args = parser.parse_args()
    
    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    try:
        pipeline = NormalizePipeline()
        result = pipeline.run()
        
        print(f"✅ Normalization completed: {result} total records")
        
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
