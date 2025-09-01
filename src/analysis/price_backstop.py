#!/usr/bin/env python3
"""Price backstop computation for luxury classification."""

import pandas as pd
import polars as pl
import duckdb
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import structlog

logger = structlog.get_logger()

class PriceBackstopAnalyzer:
    """Analyze price distributions and compute luxury thresholds."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration."""
        self.config_path = config_path
        self.brands_tiers = self._load_brands_tiers()
        
    def _load_brands_tiers(self) -> Dict[str, List[str]]:
        """Load brand tiers from reference file."""
        with open("data/reference/brands_tiers.json", "r") as f:
            data = json.load(f)
        return data["tiers"]
    
    def compute_category_price_stats(self, products_df: pd.DataFrame) -> pd.DataFrame:
        """Compute price statistics by category and site."""
        # Use DuckDB for efficient aggregation
        con = duckdb.connect()
        
        # Register the DataFrame
        con.register("products", products_df)
        
        query = """
        SELECT 
            site,
            category_path[1] as category,
            COUNT(*) as count,
            AVG(price_value) as mean_price,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_value) as p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price_value) as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_value) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY price_value) as p90,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price_value) as p95,
            MIN(price_value) as min_price,
            MAX(price_value) as max_price,
            STDDEV(price_value) as std_price
        FROM products 
        WHERE price_value > 0 AND price_value IS NOT NULL
        GROUP BY site, category_path[1]
        HAVING COUNT(*) >= 5
        ORDER BY site, p75 DESC
        """
        
        stats_df = con.execute(query).df()
        con.close()
        
        # Add metadata
        stats_df['computed_ts'] = datetime.now()
        stats_df['currency'] = 'EUR'  # Assuming EUR for French market
        
        return stats_df
    
    def classify_luxury_products(self, products_df: pd.DataFrame, price_stats_df: pd.DataFrame) -> pd.DataFrame:
        """Classify products as luxury based on brand tier and price backstop."""
        
        # Create brand tier mapping
        tier_1_brands = set(self.brands_tiers["1"])
        tier_1_5_brands = set(self.brands_tiers["1.5"])
        
        def get_brand_tier(brand: str) -> Optional[str]:
            """Get brand tier for a given brand."""
            if brand in tier_1_brands:
                return "1"
            elif brand in tier_1_5_brands:
                return "1.5"
            return None
        
        # Add brand tier
        products_df['brand_tier'] = products_df['brand'].apply(get_brand_tier)
        
        # Create price threshold mapping
        price_thresholds = {}
        for _, row in price_stats_df.iterrows():
            key = (row['site'], row['category'])
            price_thresholds[key] = row['p75']
        
        def is_luxury_by_price(row) -> bool:
            """Check if product meets price threshold for luxury."""
            if pd.isna(row['price_value']) or row['price_value'] <= 0:
                return False
            
            # Handle category_path properly (numpy array)
            category_path = row.get('category_path')
            if category_path is None or len(category_path) == 0:
                return False
            
            # Convert numpy array to list if needed
            if hasattr(category_path, '__len__'):
                category = category_path[0] if len(category_path) > 0 else None
            else:
                category = None
                
            if not category:
                return False
            
            key = (row['site'], category)
            threshold = price_thresholds.get(key)
            
            if threshold is None:
                # Fallback to global threshold
                return row['price_value'] >= 100  # Conservative fallback
            
            return row['price_value'] >= threshold
        
        # Apply luxury classification
        products_df['is_luxury'] = (
            (products_df['brand_tier'].notna()) & 
            (products_df.apply(is_luxury_by_price, axis=1))
        )
        
        return products_df
    
    def generate_kept_dropped_report(self, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Generate kept/dropped report for luxury classification."""
        
        # Kept products (luxury)
        kept_df = products_df[products_df['is_luxury'] == True].head(20)
        
        # Dropped products (non-luxury)
        dropped_df = products_df[products_df['is_luxury'] == False].head(20)
        
        # Add reason for classification
        def get_classification_reason(row) -> str:
            if row['is_luxury']:
                if row['brand_tier']:
                    return f"Brand tier {row['brand_tier']} + price threshold met"
                else:
                    return "Price threshold only"
            else:
                if not row['brand_tier']:
                    return "Brand not in luxury tiers"
                else:
                    return "Price below category threshold"
        
        kept_df['classification_reason'] = kept_df.apply(get_classification_reason, axis=1)
        dropped_df['classification_reason'] = dropped_df.apply(get_classification_reason, axis=1)
        
        return kept_df, dropped_df
    
    def save_price_stats(self, price_stats_df: pd.DataFrame, output_path: str):
        """Save price statistics to Parquet file."""
        price_stats_df.to_parquet(output_path, index=False)
        logger.info(f"Price statistics saved to {output_path}")
    
    def save_kept_dropped_report(self, kept_df: pd.DataFrame, dropped_df: pd.DataFrame, output_path: str):
        """Save kept/dropped report to CSV."""
        # Combine for easier analysis
        kept_df['status'] = 'kept'
        dropped_df['status'] = 'dropped'
        
        report_df = pd.concat([kept_df, dropped_df], ignore_index=True)
        
        # Select relevant columns for report
        report_columns = [
            'status', 'product_id', 'brand', 'name', 'price_value', 'price_currency',
            'brand_tier', 'category_path', 'classification_reason'
        ]
        
        report_df[report_columns].to_csv(output_path, index=False)
        logger.info(f"Kept/dropped report saved to {output_path}")

def main():
    """Main execution function."""
    analyzer = PriceBackstopAnalyzer()
    
    # Load comprehensive Sephora data
    products_path = "data/silver/sephora_products.parquet"
    
    if not Path(products_path).exists():
        logger.warning(f"Products file not found: {products_path}")
        logger.info("Using sample data for demonstration")
        return
    
    # Load products
    products_df = pd.read_parquet(products_path)
    logger.info(f"Loaded {len(products_df)} products")
    
    # Compute price statistics
    price_stats_df = analyzer.compute_category_price_stats(products_df)
    logger.info(f"Computed price stats for {len(price_stats_df)} category-site combinations")
    
    # Classify luxury products
    products_df = analyzer.classify_luxury_products(products_df, price_stats_df)
    
    luxury_count = products_df['is_luxury'].sum()
    total_count = len(products_df)
    logger.info(f"Classified {luxury_count}/{total_count} products as luxury ({luxury_count/total_count*100:.1f}%)")
    
    # Generate reports
    kept_df, dropped_df = analyzer.generate_kept_dropped_report(products_df)
    
    # Save outputs
    analyzer.save_price_stats(price_stats_df, "data/silver/price_stats.parquet")
    analyzer.save_kept_dropped_report(kept_df, dropped_df, "data/silver/kept_dropped_report.csv")
    
    # Save updated products with luxury classification
    products_df.to_parquet("data/silver/products_with_luxury.parquet", index=False)
    
    logger.info("Price backstop analysis complete")

if __name__ == "__main__":
    main()
