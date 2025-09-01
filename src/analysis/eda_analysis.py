#!/usr/bin/env python3
"""Comprehensive EDA analysis for thesis data exploration."""

import pandas as pd
import polars as pl
import duckdb
from pathlib import Path
import json
from datetime import datetime
import structlog

logger = structlog.get_logger()

class ThesisEDA:
    """Thesis-grade EDA analysis for luxury beauty data."""
    
    def __init__(self):
        """Initialize EDA analyzer."""
        self.con = duckdb.connect()
        
    def load_data(self):
        """Load all data files."""
        # Load comprehensive Sephora products
        products_path = "data/silver/sephora_products.parquet"
        if Path(products_path).exists():
            self.products_df = pd.read_parquet(products_path)
            logger.info(f"Loaded {len(self.products_df)} products")
        else:
            logger.warning(f"Products file not found: {products_path}")
            self.products_df = pd.DataFrame()
        
        # Load comprehensive Sephora reviews
        reviews_path = "data/silver/sephora_reviews.parquet"
        if Path(reviews_path).exists():
            self.reviews_df = pd.read_parquet(reviews_path)
            logger.info(f"Loaded {len(self.reviews_df)} reviews")
        else:
            logger.warning(f"Reviews file not found: {reviews_path}")
            self.reviews_df = pd.DataFrame()
        
        # Register with DuckDB
        if not self.products_df.empty:
            self.con.register("products", self.products_df)
        if not self.reviews_df.empty:
            self.con.register("reviews", self.reviews_df)
    
    def luxury_counts_by_brand(self):
        """Count luxury products by brand and site."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        query = """
        SELECT site, brand, COUNT(*) AS n
        FROM products
        WHERE is_luxury = true
        GROUP BY site, brand 
        ORDER BY n DESC
        """
        
        result = self.con.execute(query).df()
        logger.info(f"Luxury counts by brand: {len(result)} brands")
        return result
    
    def refillable_share_by_brand(self):
        """Refillable share by brand."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        query = """
        SELECT 
            brand, 
            ROUND(AVG(CASE WHEN refillable_flag THEN 1 ELSE 0 END), 3) AS refill_rate,
            COUNT(*) AS n
        FROM products
        WHERE is_luxury = true
        GROUP BY brand 
        HAVING n >= 1  -- Adjusted for sample data
        ORDER BY refill_rate DESC
        """
        
        result = self.con.execute(query).df()
        logger.info(f"Refillable share by brand: {len(result)} brands")
        return result
    
    def price_tiles_by_category(self):
        """Price tiles by category (thesis table)."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        query = """
        SELECT 
            site, 
            category_path[1] AS cat,
            COUNT(*) as count,
            ROUND(AVG(price_value), 2) as mean_price,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_value), 2) AS p25,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price_value), 2) AS p50,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_value), 2) AS p75,
            ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY price_value), 2) AS p90
        FROM products
        WHERE price_value > 0
        GROUP BY site, category_path[1]
        ORDER BY site, p75 DESC
        """
        
        result = self.con.execute(query).df()
        logger.info(f"Price tiles by category: {len(result)} categories")
        return result
    
    def review_health_metrics(self):
        """Review health metrics by site."""
        if self.reviews_df.empty:
            return pd.DataFrame()
        
        query = """
        SELECT 
            site, 
            COUNT(*) as n_reviews,
            ROUND(AVG(rating), 2) as avg_rating,
            ROUND(AVG(CASE WHEN language = 'fr' THEN 1 ELSE 0 END), 3) as fr_ratio,
            COUNT(DISTINCT product_id) as unique_products
        FROM reviews
        GROUP BY site
        """
        
        result = self.con.execute(query).df()
        logger.info(f"Review health metrics: {len(result)} sites")
        return result
    
    def text_sufficiency_for_nlp(self):
        """Text sufficiency analysis for NLP."""
        if self.reviews_df.empty:
            return pd.DataFrame()
        
        query = """
        SELECT 
            COUNT(*) as total_reviews,
            ROUND(AVG(LENGTH(body)), 2) AS avg_len,
            ROUND(SUM(CASE WHEN LENGTH(body) >= 120 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS share_120p,
            ROUND(SUM(CASE WHEN LENGTH(body) >= 50 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS share_50p
        FROM reviews
        WHERE language = 'fr'
        """
        
        result = self.con.execute(query).df()
        logger.info("Text sufficiency analysis complete")
        return result
    
    def luxury_vs_non_luxury_comparison(self):
        """Compare luxury vs non-luxury products."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        query = """
        SELECT 
            is_luxury,
            COUNT(*) as product_count,
            ROUND(AVG(price_value), 2) as avg_price,
            ROUND(AVG(rating_avg), 2) as avg_rating,
            ROUND(AVG(rating_count), 0) as avg_review_count,
            ROUND(AVG(CASE WHEN refillable_flag THEN 1 ELSE 0 END), 3) as refill_rate
        FROM products
        GROUP BY is_luxury
        ORDER BY is_luxury DESC
        """
        
        result = self.con.execute(query).df()
        logger.info("Luxury vs non-luxury comparison complete")
        return result
    
    def refillable_evidence_analysis(self):
        """Analyze refillable evidence patterns."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        # This requires pandas for complex list operations
        refillable_products = self.products_df[self.products_df['refillable_flag'] == True]
        
        if len(refillable_products) == 0:
            return pd.DataFrame()
        
        evidence_counts = {}
        for evidence_list in refillable_products['refill_evidence']:
            if evidence_list is not None and len(evidence_list) > 0:
                for evidence in evidence_list:
                    evidence_counts[evidence] = evidence_counts.get(evidence, 0) + 1
        
        result = pd.DataFrame([
            {'evidence_type': k, 'count': v, 'percentage': v/len(refillable_products)*100}
            for k, v in evidence_counts.items()
        ])
        
        logger.info(f"Refillable evidence analysis: {len(result)} evidence types")
        return result
    
    def generate_eda_report(self):
        """Generate comprehensive EDA report."""
        logger.info("Starting comprehensive EDA analysis...")
        
        # Run all analyses
        analyses = {
            'luxury_counts_by_brand': self.luxury_counts_by_brand(),
            'refillable_share_by_brand': self.refillable_share_by_brand(),
            'price_tiles_by_category': self.price_tiles_by_category(),
            'review_health_metrics': self.review_health_metrics(),
            'text_sufficiency_for_nlp': self.text_sufficiency_for_nlp(),
            'luxury_vs_non_luxury_comparison': self.luxury_vs_non_luxury_comparison(),
            'refillable_evidence_analysis': self.refillable_evidence_analysis()
        }
        
        # Save individual results
        for name, df in analyses.items():
            if not df.empty:
                output_path = f"data/silver/eda_{name}.csv"
                df.to_csv(output_path, index=False)
                logger.info(f"Saved {name} to {output_path}")
        
        # Generate summary report
        self._generate_summary_report(analyses)
        
        logger.info("EDA analysis complete")
        return analyses
    
    def _generate_summary_report(self, analyses):
        """Generate summary report."""
        summary = {
            'analysis_timestamp': datetime.now().isoformat(),
            'total_products': len(self.products_df),
            'total_reviews': len(self.reviews_df),
            'luxury_products': len(self.products_df[self.products_df['is_luxury'] == True]) if not self.products_df.empty else 0,
            'refillable_products': len(self.products_df[self.products_df['refillable_flag'] == True]) if not self.products_df.empty else 0,
            'french_reviews': len(self.reviews_df[self.reviews_df['language'] == 'fr']) if not self.reviews_df.empty else 0,
            'unique_brands': self.products_df['brand'].nunique() if not self.products_df.empty else 0,
            'unique_sites': self.products_df['site'].nunique() if not self.products_df.empty else 0
        }
        
        # Add analysis-specific summaries
        if not analyses['luxury_counts_by_brand'].empty:
            summary['luxury_brands'] = len(analyses['luxury_counts_by_brand'])
        
        if not analyses['price_tiles_by_category'].empty:
            summary['categories_analyzed'] = len(analyses['price_tiles_by_category'])
        
        # Save summary
        with open("data/silver/eda_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info("Summary report generated")

def main():
    """Main execution function."""
    eda = ThesisEDA()
    eda.load_data()
    
    if eda.products_df.empty and eda.reviews_df.empty:
        logger.warning("No data files found for analysis")
        return
    
    # Run comprehensive EDA
    results = eda.generate_eda_report()
    
    # Print key findings
    print("\n" + "="*60)
    print("THESIS EDA ANALYSIS RESULTS")
    print("="*60)
    
    if not eda.products_df.empty:
        luxury_count = len(eda.products_df[eda.products_df['is_luxury'] == True])
        total_products = len(eda.products_df)
        print(f"📊 Products: {total_products} total, {luxury_count} luxury ({luxury_count/total_products*100:.1f}%)")
        
        refillable_count = len(eda.products_df[eda.products_df['refillable_flag'] == True])
        print(f"♻️  Refillable: {refillable_count} products ({refillable_count/total_products*100:.1f}%)")
    
    if not eda.reviews_df.empty:
        french_reviews = len(eda.reviews_df[eda.reviews_df['language'] == 'fr'])
        total_reviews = len(eda.reviews_df)
        print(f"🇫🇷 Reviews: {total_reviews} total, {french_reviews} French ({french_reviews/total_reviews*100:.1f}%)")
    
    print("\n📁 Generated files:")
    print("- data/silver/eda_*.csv (individual analyses)")
    print("- data/silver/eda_summary.json (summary report)")
    print("="*60)

if __name__ == "__main__":
    main()
