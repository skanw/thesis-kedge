"""Tests for Pydantic schema models."""

import pytest
from datetime import datetime, date
from pydantic import ValidationError
from src.common.schema import (
    Product, Review, Brand, PageManifest, ComplianceManifest, 
    RunManifest, PriceStats, Site, Language, RefillEvidence
)


class TestProduct:
    """Test Product model validation."""
    
    def test_valid_product(self):
        """Test creating a valid product."""
        product_data = {
            "product_id": "test-123",
            "site": Site.SEPHORA,
            "url": "https://www.sephora.fr/product/test-123",
            "brand": "Chanel",
            "name": "N°5 Eau de Parfum",
            "category_path": ["fragrance", "edp"],
            "price_value": 120.50,
            "price_currency": "EUR",
            "size": "100 ml",
            "size_ml_or_g": 100.0,
            "availability": "En stock",
            "rating_avg": 4.5,
            "rating_count": 1250,
            "refillable_flag": True,
            "refill_evidence": [RefillEvidence.BADGE],
            "refill_parent_sku": None,
            "packaging_notes": "Refillable packaging",
            "ingredients_present": True,
            "ean_gtin": "1234567890123",
            "image_url": "https://www.sephora.fr/images/product.jpg",
            "breadcrumbs": ["Parfums", "Chanel", "N°5"],
            "first_seen_ts": datetime.now(),
            "last_seen_ts": datetime.now(),
            "source_site": "sephora.fr",
            "source_url": "https://www.sephora.fr/product/test-123",
            "scrape_ts": datetime.now(),
            "is_luxury": True,
            "brand_tier": "1",
            "enrichment": {"open_beauty_facts": {"ingredients": ["alcohol", "parfum"]}}
        }
        
        product = Product(**product_data)
        
        assert product.product_id == "test-123"
        assert product.brand == "Chanel"
        assert product.price_value == 120.50
        assert product.refillable_flag is True
        assert RefillEvidence.BADGE in product.refill_evidence
        assert product.is_luxury is True
        assert product.brand_tier == "1"
    
    def test_invalid_product_missing_required(self):
        """Test product creation with missing required fields."""
        product_data = {
            "product_id": "test-123",
            "site": Site.SEPHORA,
            "url": "https://www.sephora.fr/product/test-123",
            # Missing brand, name, price_value, etc.
        }
        
        with pytest.raises(ValidationError):
            Product(**product_data)
    
    def test_invalid_price(self):
        """Test product with invalid price."""
        product_data = {
            "product_id": "test-123",
            "site": Site.SEPHORA,
            "url": "https://www.sephora.fr/product/test-123",
            "brand": "Chanel",
            "name": "N°5 Eau de Parfum",
            "category_path": ["fragrance"],
            "price_value": -10.0,  # Invalid negative price
            "price_currency": "EUR",
            "first_seen_ts": datetime.now(),
            "last_seen_ts": datetime.now(),
            "source_site": "sephora.fr",
            "source_url": "https://www.sephora.fr/product/test-123",
            "scrape_ts": datetime.now()
        }
        
        with pytest.raises(ValidationError):
            Product(**product_data)
    
    def test_invalid_rating(self):
        """Test product with invalid rating."""
        product_data = {
            "product_id": "test-123",
            "site": Site.SEPHORA,
            "url": "https://www.sephora.fr/product/test-123",
            "brand": "Chanel",
            "name": "N°5 Eau de Parfum",
            "category_path": ["fragrance"],
            "price_value": 120.50,
            "price_currency": "EUR",
            "rating_avg": 6.0,  # Invalid rating > 5
            "first_seen_ts": datetime.now(),
            "last_seen_ts": datetime.now(),
            "source_site": "sephora.fr",
            "source_url": "https://www.sephora.fr/product/test-123",
            "scrape_ts": datetime.now()
        }
        
        with pytest.raises(ValidationError):
            Product(**product_data)


class TestReview:
    """Test Review model validation."""
    
    def test_valid_review(self):
        """Test creating a valid review."""
        review_data = {
            "review_id": "review-456",
            "product_id": "test-123",
            "site": "sephora",
            "url": "https://www.sephora.fr/product/test-123#review-456",
            "rating": 5,
            "title": "Excellent parfum",
            "body": "Ce parfum est vraiment magnifique. Je le recommande vivement.",
            "language": Language.FRENCH,
            "review_date": date.today(),
            "verified_purchase": True,
            "helpful_count": 15,
            "author_label": "Client vérifié",
            "scrape_ts": datetime.now()
        }
        
        review = Review(**review_data)
        
        assert review.review_id == "review-456"
        assert review.rating == 5
        assert review.language == Language.FRENCH
        assert review.verified_purchase is True
        assert review.helpful_count == 15
        assert review.author_label == "Client vérifié"
    
    def test_invalid_rating(self):
        """Test review with invalid rating."""
        review_data = {
            "review_id": "review-456",
            "product_id": "test-123",
            "site": "sephora",
            "url": "https://www.sephora.fr/product/test-123#review-456",
            "rating": 0,  # Invalid rating < 1
            "body": "Test review",
            "language": Language.FRENCH,
            "review_date": date.today(),
            "scrape_ts": datetime.now()
        }
        
        with pytest.raises(ValidationError):
            Review(**review_data)
    
    def test_invalid_language(self):
        """Test review with invalid language."""
        review_data = {
            "review_id": "review-456",
            "product_id": "test-123",
            "site": "sephora",
            "url": "https://www.sephora.fr/product/test-123#review-456",
            "rating": 5,
            "body": "Test review",
            "language": "invalid",  # Invalid language
            "review_date": date.today(),
            "scrape_ts": datetime.now()
        }
        
        with pytest.raises(ValidationError):
            Review(**review_data)


class TestBrand:
    """Test Brand model validation."""
    
    def test_valid_brand(self):
        """Test creating a valid brand."""
        brand_data = {
            "name": "Chanel",
            "tier": "1",
            "country": "France",
            "parent_company": "Chanel SAS"
        }
        
        brand = Brand(**brand_data)
        
        assert brand.name == "Chanel"
        assert brand.tier == "1"
        assert brand.country == "France"
        assert brand.parent_company == "Chanel SAS"


class TestPageManifest:
    """Test PageManifest model validation."""
    
    def test_valid_page_manifest(self):
        """Test creating a valid page manifest."""
        manifest_data = {
            "url": "https://www.sephora.fr/product/test-123",
            "site": "sephora.fr",
            "scrape_ts": datetime.now(),
            "status_code": 200,
            "content_length": 15000,
            "html_hash": "abc123",
            "robots_allowed": True,
            "crawl_delay": 1.5,
            "user_agent": "Mozilla/5.0...",
            "response_time_ms": 250.5
        }
        
        manifest = PageManifest(**manifest_data)
        
        assert manifest.status_code == 200
        assert manifest.robots_allowed is True
        assert manifest.crawl_delay == 1.5
        assert manifest.response_time_ms == 250.5


class TestComplianceManifest:
    """Test ComplianceManifest model validation."""
    
    def test_valid_compliance_manifest(self):
        """Test creating a valid compliance manifest."""
        manifest_data = {
            "domain": "sephora.fr",
            "robots_etag": "abc123",
            "robots_last_modified": "Wed, 01 Jan 2024 00:00:00 GMT",
            "allow_paths": ["/product/", "/reviews/"],
            "disallow_paths": ["/admin/", "/private/"],
            "crawl_delay": 2.0,
            "start_ts": datetime.now(),
            "end_ts": datetime.now(),
            "total_requests": 1000,
            "blocked_requests": 5,
            "rate_limit_violations": 2
        }
        
        manifest = ComplianceManifest(**manifest_data)
        
        assert manifest.domain == "sephora.fr"
        assert manifest.crawl_delay == 2.0
        assert manifest.total_requests == 1000
        assert manifest.blocked_requests == 5
        assert manifest.rate_limit_violations == 2


class TestRunManifest:
    """Test RunManifest model validation."""
    
    def test_valid_run_manifest(self):
        """Test creating a valid run manifest."""
        manifest_data = {
            "run_id": "run_20240101_120000_abc123",
            "git_hash": "abc123def456",
            "config_version": "1.0.0",
            "start_ts": datetime.now(),
            "end_ts": datetime.now(),
            "domains": ["sephora.fr", "marionnaud.fr"],
            "products_count": 1500,
            "reviews_count": 25000,
            "errors_count": 15,
            "compliance_manifests": []
        }
        
        manifest = RunManifest(**manifest_data)
        
        assert manifest.run_id == "run_20240101_120000_abc123"
        assert manifest.products_count == 1500
        assert manifest.reviews_count == 25000
        assert manifest.errors_count == 15
        assert len(manifest.domains) == 2


class TestPriceStats:
    """Test PriceStats model validation."""
    
    def test_valid_price_stats(self):
        """Test creating valid price statistics."""
        stats_data = {
            "site": "sephora",
            "category": "fragrance_edp_edt",
            "p25": 80.0,
            "p50": 120.0,
            "p75": 180.0,
            "p90": 250.0,
            "count": 500,
            "currency": "EUR",
            "computed_ts": datetime.now()
        }
        
        stats = PriceStats(**stats_data)
        
        assert stats.site == "sephora"
        assert stats.category == "fragrance_edp_edt"
        assert stats.p75 == 180.0
        assert stats.count == 500
        assert stats.currency == "EUR"


class TestEnums:
    """Test enum values."""
    
    def test_site_enum(self):
        """Test Site enum values."""
        assert Site.SEPHORA == "sephora"
        assert Site.MARIONNAUD == "marionnaud"
        assert Site.NOCIBE == "nocibe"
        assert Site.BRANDSTORE == "brandstore"
    
    def test_language_enum(self):
        """Test Language enum values."""
        assert Language.FRENCH == "fr"
        assert Language.ENGLISH == "en"
        assert Language.OTHER == "other"
    
    def test_refill_evidence_enum(self):
        """Test RefillEvidence enum values."""
        assert RefillEvidence.FACET == "facet"
        assert RefillEvidence.BADGE == "badge"
        assert RefillEvidence.ATTRIBUTE_TEXT == "attribute_text"
