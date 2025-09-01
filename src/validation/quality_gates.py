#!/usr/bin/env python3
"""Real-time quality gates for data collection monitoring."""

import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import structlog

logger = structlog.get_logger()

class QualityGates:
    """Real-time quality monitoring for data collection."""
    
    def __init__(self):
        """Initialize quality gates."""
        self.con = duckdb.connect()
        
    def check_provenance_integrity(self) -> Dict[str, Any]:
        """Check that all rows have required provenance fields."""
        logger.info("Checking provenance integrity...")
        
        try:
            # Check products
            products_missing = self.con.execute("""
                SELECT COUNT(*) AS missing_count
                FROM read_parquet('data/silver/products.parquet')
                WHERE source_url IS NULL OR scrape_ts IS NULL OR robots_snapshot_id IS NULL
            """).df().iloc[0, 0]
            
            # Check reviews
            reviews_missing = self.con.execute("""
                SELECT COUNT(*) AS missing_count
                FROM read_parquet('data/silver/reviews.parquet')
                WHERE source_url IS NULL OR scrape_ts IS NULL OR robots_snapshot_id IS NULL
            """).df().iloc[0, 0]
            
            result = {
                "products_missing_provenance": products_missing,
                "reviews_missing_provenance": reviews_missing,
                "status": "PASS" if (products_missing == 0 and reviews_missing == 0) else "FAIL"
            }
            
            if result["status"] == "PASS":
                logger.info("✅ All rows have required provenance fields")
            else:
                logger.error(f"❌ Missing provenance: {products_missing} products, {reviews_missing} reviews")
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking provenance: {e}")
            return {"status": "ERROR", "message": str(e)}
    
    def check_luxury_coverage(self) -> Dict[str, Any]:
        """Check luxury coverage by site."""
        logger.info("Checking luxury coverage...")
        
        try:
            luxury_stats = self.con.execute("""
                SELECT 
                    site, 
                    COUNT(*) AS total_products,
                    COUNT(CASE WHEN is_luxury = true THEN 1 END) AS luxury_products,
                    ROUND(AVG(CASE WHEN is_luxury = true THEN 1.0 ELSE 0.0 END), 3) AS luxury_rate,
                    ROUND(AVG(price_value), 2) AS avg_price
                FROM read_parquet('data/silver/products.parquet')
                GROUP BY site
                ORDER BY total_products DESC
            """).df()
            
            result = {
                "luxury_stats": luxury_stats.to_dict('records'),
                "status": "PASS" if len(luxury_stats) > 0 else "FAIL"
            }
            
            logger.info(f"Luxury coverage: {len(luxury_stats)} sites")
            for _, row in luxury_stats.iterrows():
                logger.info(f"  {row['site']}: {row['luxury_products']}/{row['total_products']} luxury ({row['luxury_rate']:.1%})")
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking luxury coverage: {e}")
            return {"status": "ERROR", "message": str(e)}
    
    def check_refillable_evidence(self) -> Dict[str, Any]:
        """Check refillable evidence integrity."""
        logger.info("Checking refillable evidence...")
        
        try:
            refillable_issues = self.con.execute("""
                SELECT COUNT(*) AS invalid_count
                FROM read_parquet('data/silver/products.parquet')
                WHERE refillable_flag = TRUE 
                AND (refill_evidence IS NULL OR array_length(refill_evidence) = 0)
            """).df().iloc[0, 0]
            
            total_refillable = self.con.execute("""
                SELECT COUNT(*) AS total_count
                FROM read_parquet('data/silver/products.parquet')
                WHERE refillable_flag = TRUE
            """).df().iloc[0, 0]
            
            result = {
                "invalid_refillable": refillable_issues,
                "total_refillable": total_refillable,
                "status": "PASS" if refillable_issues == 0 else "FAIL"
            }
            
            if result["status"] == "PASS":
                logger.info(f"✅ All {total_refillable} refillable products have evidence")
            else:
                logger.error(f"❌ {refillable_issues}/{total_refillable} refillable products missing evidence")
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking refillable evidence: {e}")
            return {"status": "ERROR", "message": str(e)}
    
    def check_review_quality(self) -> Dict[str, Any]:
        """Check review language mix and freshness."""
        logger.info("Checking review quality...")
        
        try:
            review_stats = self.con.execute("""
                SELECT 
                    site,
                    COUNT(*) AS total_reviews,
                    ROUND(AVG(CASE WHEN language = 'fr' THEN 1.0 ELSE 0.0 END), 3) AS fr_ratio,
                    MIN(review_date) AS min_date,
                    MAX(review_date) AS max_date,
                    ROUND(AVG(rating), 2) AS avg_rating
                FROM read_parquet('data/silver/reviews.parquet')
                GROUP BY site
                ORDER BY total_reviews DESC
            """).df()
            
            result = {
                "review_stats": review_stats.to_dict('records'),
                "status": "PASS" if len(review_stats) > 0 else "FAIL"
            }
            
            logger.info(f"Review quality: {len(review_stats)} sites")
            for _, row in review_stats.iterrows():
                logger.info(f"  {row['site']}: {row['total_reviews']} reviews, {row['fr_ratio']:.1%} French, avg {row['avg_rating']}/5")
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking review quality: {e}")
            return {"status": "ERROR", "message": str(e)}
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all quality gate checks."""
        logger.info("🚀 Running all quality gate checks...")
        
        results = {
            "provenance": self.check_provenance_integrity(),
            "luxury_coverage": self.check_luxury_coverage(),
            "refillable_evidence": self.check_refillable_evidence(),
            "review_quality": self.check_review_quality()
        }
        
        # Overall status
        all_passed = all(result.get("status") == "PASS" for result in results.values())
        results["overall_status"] = "PASS" if all_passed else "FAIL"
        
        logger.info(f"Quality gates: {results['overall_status']}")
        
        return results

def main():
    """Main function for running quality gates."""
    gates = QualityGates()
    results = gates.run_all_checks()
    
    print("\n" + "="*60)
    print("🔍 QUALITY GATES RESULTS")
    print("="*60)
    print(f"Overall Status: {results['overall_status']}")
    print("="*60)
    
    for check_name, result in results.items():
        if check_name == "overall_status":
            continue
        print(f"\n{check_name.upper()}: {result.get('status', 'UNKNOWN')}")
        if result.get("status") == "FAIL":
            print(f"  Issues found - check logs for details")

if __name__ == "__main__":
    main()
