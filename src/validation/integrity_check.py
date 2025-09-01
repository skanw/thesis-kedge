#!/usr/bin/env python3
"""Integrity check to enforce provenance gates and detect synthetic data."""

import duckdb
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import structlog

logger = structlog.get_logger()

class IntegrityChecker:
    """Enforce provenance gates and detect synthetic data."""
    
    def __init__(self):
        """Initialize integrity checker."""
        self.con = duckdb.connect()
        self.fixtures_dir = Path("data/fixtures")
        self.silver_dir = Path("data/silver")
        self.violations = []
        
    def check_file_existence(self):
        """Check if required files exist and are Parquet."""
        logger.info("Checking file existence...")
        
        required_files = [
            "data/silver/products.parquet",
            "data/silver/reviews.parquet"
        ]
        
        for file_path in required_files:
            if not Path(file_path).exists():
                self.violations.append(f"Missing required file: {file_path}")
                logger.error(f"Missing required file: {file_path}")
            elif not file_path.endswith('.parquet'):
                self.violations.append(f"File not in Parquet format: {file_path}")
                logger.error(f"File not in Parquet format: {file_path}")
            else:
                logger.info(f"✅ Found: {file_path}")
    
    def check_provenance_gates(self):
        """Enforce provenance gates - non-negotiable fields."""
        logger.info("Checking provenance gates...")
        
        # Check products table
        try:
            missing_core = self.con.execute("""
                SELECT COUNT(*) AS missing_core
                FROM read_parquet('data/silver/products.parquet')
                WHERE source_url IS NULL OR scrape_ts IS NULL
            """).df()
            
            if missing_core.iloc[0]['missing_core'] > 0:
                self.violations.append(f"Products missing core provenance: {missing_core.iloc[0]['missing_core']} rows")
                logger.error(f"Products missing core provenance: {missing_core.iloc[0]['missing_core']} rows")
            else:
                logger.info("✅ All products have core provenance fields")
                
        except Exception as e:
            self.violations.append(f"Error checking products provenance: {e}")
            logger.error(f"Error checking products provenance: {e}")
        
        # Check reviews table
        try:
            missing_core = self.con.execute("""
                SELECT COUNT(*) AS missing_core
                FROM read_parquet('data/silver/reviews.parquet')
                WHERE source_url IS NULL OR scrape_ts IS NULL
            """).df()
            
            if missing_core.iloc[0]['missing_core'] > 0:
                self.violations.append(f"Reviews missing core provenance: {missing_core.iloc[0]['missing_core']} rows")
                logger.error(f"Reviews missing core provenance: {missing_core.iloc[0]['missing_core']} rows")
            else:
                logger.info("✅ All reviews have core provenance fields")
                
        except Exception as e:
            self.violations.append(f"Error checking reviews provenance: {e}")
            logger.error(f"Error checking reviews provenance: {e}")
    
    def check_refillable_evidence(self):
        """Check refillable evidence requirements."""
        logger.info("Checking refillable evidence...")
        
        try:
            invalid_refillable = self.con.execute("""
                SELECT COUNT(*) AS invalid_count
                FROM read_parquet('data/silver/products.parquet')
                WHERE refillable_flag = true 
                AND (refill_evidence IS NULL OR array_length(refill_evidence) = 0)
            """).df()
            
            if invalid_refillable.iloc[0]['invalid_count'] > 0:
                self.violations.append(f"Refillable products without evidence: {invalid_refillable.iloc[0]['invalid_count']} rows")
                logger.error(f"Refillable products without evidence: {invalid_refillable.iloc[0]['invalid_count']} rows")
            else:
                logger.info("✅ All refillable products have evidence")
                
        except Exception as e:
            self.violations.append(f"Error checking refillable evidence: {e}")
            logger.error(f"Error checking refillable evidence: {e}")
    
    def check_robots_provenance(self):
        """Check robots.txt provenance."""
        logger.info("Checking robots provenance...")
        
        manifest_path = Path("data/silver/manifest_runs.parquet")
        if not manifest_path.exists():
            self.violations.append("Missing manifest_runs.parquet")
            logger.error("Missing manifest_runs.parquet")
            return
        
        try:
            missing_robots = self.con.execute("""
                SELECT site, COUNT(*) AS n
                FROM read_parquet('data/silver/manifest_runs.parquet')
                WHERE robots_etag IS NULL OR robots_path IS NULL
                GROUP BY site
            """).df()
            
            if not missing_robots.empty:
                for _, row in missing_robots.iterrows():
                    self.violations.append(f"Site {row['site']} missing robots provenance: {row['n']} records")
                    logger.error(f"Site {row['site']} missing robots provenance: {row['n']} records")
            else:
                logger.info("✅ All sites have robots provenance")
                
        except Exception as e:
            self.violations.append(f"Error checking robots provenance: {e}")
            logger.error(f"Error checking robots provenance: {e}")
    
    def check_synthetic_indicators(self):
        """Check for patterns that indicate synthetic data."""
        logger.info("Checking for synthetic data indicators...")
        
        # 1) Brand generator pattern check
        try:
            brand_query = """
            SELECT COUNT(*) as synthetic_brands
            FROM read_parquet('data/silver/products.parquet')
            WHERE brand ~ '^Brand_[0-9]+$'
            """
            synthetic_brands = self.con.execute(brand_query).fetchone()[0]
            if synthetic_brands > 0:
                self.violations.append(f"Found {synthetic_brands} synthetic brand names (Brand_0 pattern)")
                logger.error("Synthetic brand pattern detected", count=synthetic_brands)
        except Exception as e:
            logger.error("Error checking brand patterns", error=str(e))
            self.violations.append(f"Brand pattern check failed: {str(e)}")
        
        # 2) Luxury brand overlap check
        try:
            luxury_brands = [
                'Chanel', 'Parfums Christian Dior', 'Guerlain', 'Hermès', 'Givenchy',
                'Maison Francis Kurkdjian', 'Acqua di Parma', 'Tom Ford Beauty',
                'La Mer', 'Jo Malone London', 'Le Labo', 'By Kilian', 'Yves Saint Laurent',
                'Lancôme', 'Armani Beauty', 'Valentino Beauty', 'Burberry Beauty', 'Chloé'
            ]
            luxury_query = f"""
            SELECT COUNT(*) as luxury_count
            FROM read_parquet('data/silver/products.parquet')
            WHERE brand IN ({','.join([f"'{b}'" for b in luxury_brands])})
            """
            luxury_count = self.con.execute(luxury_query).fetchone()[0]
            if luxury_count == 0:
                self.violations.append("No luxury brands found - data appears synthetic")
                logger.error("No luxury brands detected", expected_brands=len(luxury_brands))
            else:
                logger.info("Luxury brands found", count=luxury_count)
        except Exception as e:
            logger.error("Error checking luxury brands", error=str(e))
            self.violations.append(f"Luxury brand check failed: {str(e)}")
        
        # 3) Price gap uniformity check
        try:
            price_query = """
            WITH p AS (
                SELECT DISTINCT price_value 
                FROM read_parquet('data/silver/products.parquet')
                WHERE price_value IS NOT NULL
                ORDER BY price_value
            ),
            g AS (
                SELECT LEAD(price_value) OVER (ORDER BY price_value) - price_value AS gap 
                FROM p
            )
            SELECT COUNT(DISTINCT ROUND(gap,2)) AS distinct_gaps
            FROM g WHERE gap IS NOT NULL
            """
            distinct_gaps = self.con.execute(price_query).fetchone()[0]
            if distinct_gaps <= 1:
                self.violations.append(f"Price gaps too uniform: only {distinct_gaps} distinct gaps")
                logger.error("Suspicious price uniformity", distinct_gaps=distinct_gaps)
            else:
                logger.info("Price distribution looks natural", distinct_gaps=distinct_gaps)
        except Exception as e:
            logger.error("Error checking price gaps", error=str(e))
            self.violations.append(f"Price gap check failed: {str(e)}")
        
        # 4) Review duplication check
        try:
            review_query = """
            WITH dup_check AS (
                SELECT rating, text, COUNT(*) as dup_count
                FROM read_parquet('data/silver/reviews.parquet')
                GROUP BY rating, text
                HAVING COUNT(*) > 1
            )
            SELECT 
                SUM(dup_count) as total_duplicates,
                COUNT(*) as duplicate_pairs,
                (SELECT COUNT(*) FROM read_parquet('data/silver/reviews.parquet')) as total_reviews
            FROM dup_check
            """
            dup_result = self.con.execute(review_query).fetchone()
            if dup_result:
                total_duplicates, duplicate_pairs, total_reviews = dup_result
                dup_percentage = (total_duplicates / total_reviews) * 100 if total_reviews > 0 else 0
                if dup_percentage > 10:
                    self.violations.append(f"High review duplication: {dup_percentage:.1f}% ({duplicate_pairs} duplicate pairs)")
                    logger.error("Suspicious review duplication", percentage=dup_percentage, pairs=duplicate_pairs)
                else:
                    logger.info("Review duplication within normal range", percentage=dup_percentage)
        except Exception as e:
            logger.error("Error checking review duplication", error=str(e))
            self.violations.append(f"Review duplication check failed: {str(e)}")
        
        # 5) Refillable evidence integrity check
        try:
            refill_query = """
            SELECT COUNT(*) as invalid_refill
            FROM read_parquet('data/silver/products.parquet')
            WHERE refillable_flag = TRUE 
            AND (refill_evidence IS NULL OR refill_evidence = '[]' OR refill_evidence = '')
            """
            invalid_refill = self.con.execute(refill_query).fetchone()[0]
            if invalid_refill > 0:
                self.violations.append(f"Found {invalid_refill} refillable products without evidence")
                logger.error("Invalid refillable evidence", count=invalid_refill)
        except Exception as e:
            logger.error("Error checking refillable evidence", error=str(e))
            self.violations.append(f"Refillable evidence check failed: {str(e)}")
        
        # 6) Manifest plausibility check
        try:
            manifest_query = """
            SELECT site, total_requests, blocked_requests
            FROM read_parquet('data/silver/manifest_runs.parquet')
            """
            manifest_results = self.con.execute(manifest_query).fetchall()
            
            # Get product and review counts
            count_query = """
            SELECT 
                (SELECT COUNT(*) FROM read_parquet('data/silver/products.parquet')) as product_count,
                (SELECT COUNT(*) FROM read_parquet('data/silver/reviews.parquet')) as review_count
            """
            counts = self.con.execute(count_query).fetchone()
            product_count, review_count = counts if counts else (0, 0)
            
            for site, total_requests, blocked_requests in manifest_results:
                # Rule: total_requests should be much larger than product_count for real crawling
                if total_requests < product_count * 2:  # At least 2 requests per product (discovery + details)
                    self.violations.append(f"Manifest implausible: {total_requests} requests for {product_count} products")
                    logger.error("Suspicious manifest", site=site, requests=total_requests, products=product_count)
                else:
                    logger.info("Manifest looks plausible", site=site, requests=total_requests, products=product_count)
        except Exception as e:
            logger.error("Error checking manifest plausibility", error=str(e))
            self.violations.append(f"Manifest check failed: {str(e)}")
        
        # 7) Check rating bounds (keep existing logic)
        try:
            rating_bounds = self.con.execute("""
                SELECT ROUND(AVG(CASE WHEN rating BETWEEN 1 AND 5 THEN 1.0 ELSE 0.0 END),3) AS rating_in_bounds,
                       COUNT(*) AS total_reviews
                FROM read_parquet('data/silver/reviews.parquet')
            """).df()
            
            row = rating_bounds.iloc[0]
            if row['rating_in_bounds'] == 1.0:
                logger.info("✅ All ratings within valid bounds")
            else:
                self.violations.append(f"Invalid ratings found: {row['rating_in_bounds']} in bounds")
                logger.warning(f"Invalid ratings found: {row['rating_in_bounds']} in bounds")
                
        except Exception as e:
            self.violations.append(f"Error checking rating bounds: {e}")
            logger.error(f"Error checking rating bounds: {e}")
    
    def check_price_sanity(self):
        """Check price distributions for sanity."""
        logger.info("Checking price sanity...")
        
        try:
            price_stats = self.con.execute("""
                SELECT brand,
                       COUNT(*) n, 
                       MIN(price_value) pmin, MAX(price_value) pmax,
                       COUNT(DISTINCT price_value) AS price_levels,
                       ROUND(AVG(price_value), 2) AS avg_price
                FROM read_parquet('data/silver/products.parquet')
                GROUP BY 1 HAVING n>=10
                ORDER BY price_levels ASC, n DESC
            """).df()
            
            # Check for suspicious patterns
            for _, row in price_stats.iterrows():
                if row['price_levels'] <= 2 and row['n'] >= 20:
                    self.violations.append(f"Brand {row['brand']} has suspiciously few price levels: {row['price_levels']} for {row['n']} products")
                    logger.warning(f"Brand {row['brand']} has suspiciously few price levels: {row['price_levels']} for {row['n']} products")
                
                if row['pmin'] == row['pmax'] and row['n'] >= 10:
                    self.violations.append(f"Brand {row['brand']} has identical min/max prices: {row['pmin']}")
                    logger.warning(f"Brand {row['brand']} has identical min/max prices: {row['pmin']}")
            
            logger.info(f"Price sanity check completed for {len(price_stats)} brands")
            
        except Exception as e:
            self.violations.append(f"Error checking price sanity: {e}")
            logger.error(f"Error checking price sanity: {e}")
    
    def check_fixture_contamination(self):
        """Check for fixture contamination in silver data."""
        logger.info("Checking for fixture contamination...")
        
        # Check if any files in silver have is_fixture=true
        try:
            fixture_contamination = self.con.execute("""
                SELECT COUNT(*) AS fixture_count
                FROM read_parquet('data/silver/products.parquet')
                WHERE is_fixture = true
            """).df()
            
            if fixture_contamination.iloc[0]['fixture_count'] > 0:
                self.violations.append(f"Fixture contamination in products: {fixture_contamination.iloc[0]['fixture_count']} rows")
                logger.error(f"Fixture contamination in products: {fixture_contamination.iloc[0]['fixture_count']} rows")
            else:
                logger.info("✅ No fixture contamination in products")
                
        except Exception as e:
            # Column might not exist, which is fine
            logger.info("No is_fixture column found in products (expected)")
        
        try:
            fixture_contamination = self.con.execute("""
                SELECT COUNT(*) AS fixture_count
                FROM read_parquet('data/silver/reviews.parquet')
                WHERE is_fixture = true
            """).df()
            
            if fixture_contamination.iloc[0]['fixture_count'] > 0:
                self.violations.append(f"Fixture contamination in reviews: {fixture_contamination.iloc[0]['fixture_count']} rows")
                logger.error(f"Fixture contamination in reviews: {fixture_contamination.iloc[0]['fixture_count']} rows")
            else:
                logger.info("✅ No fixture contamination in reviews")
                
        except Exception as e:
            # Column might not exist, which is fine
            logger.info("No is_fixture column found in reviews (expected)")
    
    def generate_audit_sample(self):
        """Generate audit sample for verification."""
        logger.info("Generating audit sample...")
        
        try:
            # Get 20 random products with source URLs
            audit_sample = self.con.execute("""
                SELECT product_id, brand, name, price_value, source_url, scrape_ts
                FROM read_parquet('data/silver/products.parquet')
                WHERE source_url IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 20
            """).df()
            
            audit_file = Path("data/silver/audit_sample.json")
            audit_sample.to_json(audit_file, orient='records', indent=2)
            
            logger.info(f"✅ Audit sample saved to {audit_file}")
            logger.info(f"Sample size: {len(audit_sample)} products")
            
            return audit_sample
            
        except Exception as e:
            self.violations.append(f"Error generating audit sample: {e}")
            logger.error(f"Error generating audit sample: {e}")
            return None
    
    def run_integrity_check(self):
        """Run complete integrity check."""
        logger.info("🚀 Starting integrity check...")
        
        self.check_file_existence()
        self.check_provenance_gates()
        self.check_refillable_evidence()
        self.check_robots_provenance()
        self.check_synthetic_indicators()
        self.check_price_sanity()
        self.check_fixture_contamination()
        
        audit_sample = self.generate_audit_sample()
        
        # Generate integrity report
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_violations": len(self.violations),
            "violations": self.violations,
            "audit_sample_size": len(audit_sample) if audit_sample is not None else 0,
            "status": "PASS" if len(self.violations) == 0 else "FAIL"
        }
        
        # Save report
        report_file = Path("data/silver/integrity_report.json")
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"📋 Integrity check completed: {len(self.violations)} violations")
        logger.info(f"📄 Report saved to {report_file}")
        
        if len(self.violations) > 0:
            logger.error("❌ Integrity check FAILED")
            for violation in self.violations:
                logger.error(f"  - {violation}")
        else:
            logger.info("✅ Integrity check PASSED")
        
        return report

def main():
    """Main function."""
    checker = IntegrityChecker()
    report = checker.run_integrity_check()
    
    print("\n" + "="*60)
    print("🔍 INTEGRITY CHECK RESULTS")
    print("="*60)
    print(f"Status: {report['status']}")
    print(f"Violations: {report['total_violations']}")
    print(f"Audit Sample: {report['audit_sample_size']} products")
    print("="*60)
    
    if report['total_violations'] > 0:
        print("\n❌ VIOLATIONS FOUND:")
        for violation in report['violations']:
            print(f"  - {violation}")
    else:
        print("\n✅ All integrity checks passed!")
    
    print(f"\n📄 Full report: data/silver/integrity_report.json")

if __name__ == "__main__":
    main()
