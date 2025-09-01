"""Utility functions for timestamping, hashing, and manifest management."""

import hashlib
import json
import uuid
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional, List
import yaml
import structlog
from .schema import RunManifest, ComplianceManifest

logger = structlog.get_logger(__name__)


class TimestampManager:
    """Timestamp management utilities."""
    
    @staticmethod
    def generate_run_id() -> str:
        """Generate a unique run identifier."""
        return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    @staticmethod
    def get_current_timestamp() -> datetime:
        """Get current timestamp."""
        return datetime.now()
    
    @staticmethod
    def format_timestamp(dt: datetime) -> str:
        """Format timestamp for file naming."""
        return dt.strftime("%Y%m%d_%H%M%S")
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[date]:
        """Parse date string in various formats."""
        date_formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d"
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None


class HashManager:
    """Hash management utilities."""
    
    @staticmethod
    def hash_content(content: str) -> str:
        """Generate SHA-256 hash of content."""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def hash_product_key(brand: str, name: str, size: Optional[str], site: str) -> str:
        """Generate hash key for product deduplication."""
        key_parts = [brand.lower().strip(), name.lower().strip()]
        if size:
            key_parts.append(size.lower().strip())
        key_parts.append(site.lower().strip())
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()
    
    @staticmethod
    def hash_review_content(product_id: str, rating: int, review_date: date, body_preview: str) -> str:
        """Generate hash key for review deduplication."""
        # Use first 120 characters of body for preview
        body_preview = body_preview[:120] if body_preview else ""
        
        key_parts = [
            product_id,
            str(rating),
            review_date.isoformat(),
            body_preview.lower().strip()
        ]
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()


class ManifestWriter:
    """Manifest writing utilities."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_run_manifest(self, manifest: RunManifest, format: str = "json") -> Path:
        """Write run manifest to file."""
        timestamp = TimestampManager.format_timestamp(manifest.start_ts)
        filename = f"run_manifest_{manifest.run_id}_{timestamp}.{format}"
        filepath = self.output_dir / filename
        
        if format == "json":
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(manifest.model_dump(), f, indent=2, default=str)
        elif format == "yaml":
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(manifest.model_dump(), f, default_flow_style=False, default=str)
        
        logger.info("Run manifest written", filepath=str(filepath))
        return filepath
    
    def write_compliance_manifest(self, manifest: ComplianceManifest, format: str = "json") -> Path:
        """Write compliance manifest to file."""
        timestamp = TimestampManager.format_timestamp(manifest.start_ts)
        filename = f"compliance_{manifest.domain}_{timestamp}.{format}"
        filepath = self.output_dir / filename
        
        if format == "json":
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(manifest.model_dump(), f, indent=2, default=str)
        elif format == "yaml":
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(manifest.model_dump(), f, default_flow_style=False, default=str)
        
        logger.info("Compliance manifest written", filepath=str(filepath))
        return filepath
    
    def write_summary_report(self, run_manifest: RunManifest, 
                           products_count: int, reviews_count: int,
                           refillable_count: int, luxury_count: int) -> Path:
        """Write summary report."""
        timestamp = TimestampManager.format_timestamp(run_manifest.start_ts)
        filename = f"summary_report_{run_manifest.run_id}_{timestamp}.md"
        filepath = self.output_dir / filename
        
        summary = f"""# Luxury Beauty Scraping Summary Report

## Run Information
- **Run ID**: {run_manifest.run_id}
- **Start Time**: {run_manifest.start_ts}
- **End Time**: {run_manifest.end_ts or 'In Progress'}
- **Domains Crawled**: {', '.join(run_manifest.domains)}

## Data Statistics
- **Products Scraped**: {products_count}
- **Reviews Scraped**: {reviews_count}
- **Refillable Products**: {refillable_count}
- **Luxury Products**: {luxury_count}

## Compliance Summary
"""
        
        for compliance in run_manifest.compliance_manifests:
            summary += f"""
### {compliance.domain}
- **Total Requests**: {compliance.total_requests}
- **Blocked Requests**: {compliance.blocked_requests}
- **Rate Limit Violations**: {compliance.rate_limit_violations}
- **Crawl Delay**: {compliance.crawl_delay}s
"""
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        logger.info("Summary report written", filepath=str(filepath))
        return filepath


class ConfigManager:
    """Configuration management utilities."""
    
    @staticmethod
    def load_config(config_path: Path) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info("Configuration loaded", config_path=str(config_path))
            return config
        except Exception as e:
            logger.error("Failed to load configuration", config_path=str(config_path), error=str(e))
            raise
    
    @staticmethod
    def load_brands_tiers(brands_file: Path) -> Dict[str, List[str]]:
        """Load brand tiers from JSON file."""
        try:
            with open(brands_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info("Brand tiers loaded", brands_file=str(brands_file))
            return data.get("tiers", {})
        except Exception as e:
            logger.error("Failed to load brand tiers", brands_file=str(brands_file), error=str(e))
            raise
    
    @staticmethod
    def load_categories_map(categories_file: Path) -> Dict[str, Any]:
        """Load categories mapping from JSON file."""
        try:
            with open(categories_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info("Categories map loaded", categories_file=str(categories_file))
            return data.get("mappings", {})
        except Exception as e:
            logger.error("Failed to load categories map", categories_file=str(categories_file), error=str(e))
            raise


class DataValidator:
    """Data validation utilities."""
    
    @staticmethod
    def validate_product_data(product_data: Dict[str, Any]) -> List[str]:
        """Validate product data and return list of errors."""
        errors = []
        
        required_fields = ['product_id', 'brand', 'name', 'price_value', 'price_currency']
        for field in required_fields:
            if not product_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate price
        price = product_data.get('price_value')
        if price is not None and (not isinstance(price, (int, float)) or price <= 0):
            errors.append("Invalid price value")
        
        # Validate rating
        rating = product_data.get('rating_avg')
        if rating is not None and (not isinstance(rating, (int, float)) or not (0 <= rating <= 5)):
            errors.append("Invalid rating value")
        
        return errors
    
    @staticmethod
    def validate_review_data(review_data: Dict[str, Any]) -> List[str]:
        """Validate review data and return list of errors."""
        errors = []
        
        required_fields = ['review_id', 'product_id', 'rating', 'body']
        for field in required_fields:
            if not review_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate rating
        rating = review_data.get('rating')
        if rating is not None and (not isinstance(rating, int) or not (1 <= rating <= 5)):
            errors.append("Invalid rating value")
        
        # Validate language
        language = review_data.get('language')
        if language and language not in ['fr', 'en', 'other']:
            errors.append("Invalid language value")
        
        return errors


class FileManager:
    """File management utilities."""
    
    @staticmethod
    def ensure_directory(path: Path) -> Path:
        """Ensure directory exists."""
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @staticmethod
    def save_html_snapshot(html_content: str, url: str, failures_dir: Path) -> Optional[Path]:
        """Save HTML snapshot for debugging."""
        try:
            # Create filename from URL
            url_hash = HashManager.hash_content(url)
            filename = f"snapshot_{url_hash[:8]}.html"
            filepath = failures_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"<!-- URL: {url} -->\n")
                f.write(f"<!-- Timestamp: {datetime.now().isoformat()} -->\n")
                f.write(html_content)
            
            logger.debug("HTML snapshot saved", filepath=str(filepath))
            return filepath
            
        except Exception as e:
            logger.error("Failed to save HTML snapshot", url=url, error=str(e))
            return None
    
    @staticmethod
    def cleanup_old_files(directory: Path, max_age_hours: int = 24) -> int:
        """Clean up old files in directory."""
        if not directory.exists():
            return 0
        
        cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
        deleted_count = 0
        
        for filepath in directory.iterdir():
            if filepath.is_file():
                if filepath.stat().st_mtime < cutoff_time:
                    try:
                        filepath.unlink()
                        deleted_count += 1
                    except Exception as e:
                        logger.warning("Failed to delete old file", filepath=str(filepath), error=str(e))
        
        if deleted_count > 0:
            logger.info("Cleaned up old files", directory=str(directory), deleted_count=deleted_count)
        
        return deleted_count
