"""Pydantic schema models for luxury beauty data pipeline."""

from datetime import datetime, date
from typing import Literal, Optional, List
from pydantic import BaseModel, HttpUrl, conint, Field
from enum import Enum


class RefillEvidence(str, Enum):
    """Evidence types for refillable detection."""
    FACET = "facet"
    BADGE = "badge"
    ATTRIBUTE_TEXT = "attribute_text"


class Site(str, Enum):
    """Supported e-commerce sites."""
    SEPHORA = "sephora"
    MARIONNAUD = "marionnaud"
    NOCIBE = "nocibe"
    BRANDSTORE = "brandstore"


class Language(str, Enum):
    """Supported languages."""
    FRENCH = "fr"
    ENGLISH = "en"
    OTHER = "other"


class Product(BaseModel):
    """Product metadata model."""
    product_id: str = Field(..., description="Stable product identifier")
    site: Site = Field(..., description="Source e-commerce site")
    url: HttpUrl = Field(..., description="Product page URL")
    canonical_url: Optional[HttpUrl] = Field(None, description="Canonical URL if different")
    brand: str = Field(..., description="Brand name")
    line: Optional[str] = Field(None, description="Product line/collection")
    name: str = Field(..., description="Product name")
    category_path: List[str] = Field(..., description="Category hierarchy")
    price_value: float = Field(..., description="Price value")
    price_currency: str = Field(..., description="Price currency code")
    size: Optional[str] = Field(None, description="Size as displayed")
    size_ml_or_g: Optional[float] = Field(None, description="Size in ml or grams")
    availability: Optional[str] = Field(None, description="Availability status")
    rating_avg: Optional[float] = Field(None, ge=0, le=5, description="Average rating")
    rating_count: Optional[int] = Field(None, ge=0, description="Number of ratings")
    refillable_flag: bool = Field(False, description="Whether product is refillable")
    refill_evidence: List[RefillEvidence] = Field(default_factory=list, description="Evidence for refillable flag")
    refill_parent_sku: Optional[str] = Field(None, description="Parent SKU if this is a refill")
    packaging_notes: Optional[str] = Field(None, description="Packaging information")
    ingredients_present: Optional[bool] = Field(None, description="Whether ingredients are listed")
    ean_gtin: Optional[str] = Field(None, description="EAN/GTIN barcode")
    image_url: Optional[HttpUrl] = Field(None, description="Product image URL")
    breadcrumbs: List[str] = Field(default_factory=list, description="Breadcrumb navigation")
    first_seen_ts: datetime = Field(..., description="First time product was seen")
    last_seen_ts: datetime = Field(..., description="Last time product was seen")
    source_site: str = Field(..., description="Source site domain")
    source_url: HttpUrl = Field(..., description="Source URL")
    scrape_ts: datetime = Field(..., description="Scraping timestamp")
    
    # Luxury classification
    is_luxury: Optional[bool] = Field(None, description="Luxury classification based on brand tier and price")
    brand_tier: Optional[str] = Field(None, description="Brand tier (1, 1.5, etc.)")
    
    # Enrichment fields
    enrichment: Optional[dict] = Field(None, description="Additional enrichment data")


class Review(BaseModel):
    """Review data model."""
    review_id: str = Field(..., description="Unique review identifier")
    product_id: str = Field(..., description="Associated product ID")
    site: str = Field(..., description="Source site")
    url: HttpUrl = Field(..., description="Review page URL")
    rating: conint(ge=1, le=5) = Field(..., description="Rating (1-5 stars)")
    title: Optional[str] = Field(None, description="Review title")
    body: str = Field(..., description="Review body text")
    language: Language = Field(..., description="Review language")
    review_date: date = Field(..., description="Review date")
    verified_purchase: Optional[bool] = Field(None, description="Whether purchase was verified")
    helpful_count: Optional[int] = Field(None, ge=0, description="Number of helpful votes")
    author_label: Optional[str] = Field(None, description="Anonymized author label (never PII)")
    scrape_ts: datetime = Field(..., description="Scraping timestamp")
    
    # Sentiment analysis placeholders
    sentiment_score: Optional[float] = Field(None, description="Sentiment analysis score")
    sentiment_label: Optional[str] = Field(None, description="Sentiment label")


class Brand(BaseModel):
    """Brand information model."""
    name: str = Field(..., description="Brand name")
    tier: str = Field(..., description="Luxury tier (1, 1.5, etc.)")
    country: Optional[str] = Field(None, description="Brand origin country")
    parent_company: Optional[str] = Field(None, description="Parent company if applicable")


class PageManifest(BaseModel):
    """Page scraping manifest for auditability."""
    url: HttpUrl = Field(..., description="Page URL")
    site: str = Field(..., description="Source site")
    scrape_ts: datetime = Field(..., description="Scraping timestamp")
    status_code: int = Field(..., description="HTTP status code")
    content_length: Optional[int] = Field(None, description="Content length in bytes")
    html_hash: Optional[str] = Field(None, description="HTML content hash for forensic checks")
    robots_allowed: bool = Field(..., description="Whether robots.txt allowed this URL")
    crawl_delay: Optional[float] = Field(None, description="Crawl delay from robots.txt")
    user_agent: str = Field(..., description="User agent used")
    response_time_ms: Optional[float] = Field(None, description="Response time in milliseconds")


class ComplianceManifest(BaseModel):
    """Compliance manifest for each domain."""
    domain: str = Field(..., description="Domain name")
    robots_etag: Optional[str] = Field(None, description="Robots.txt ETag")
    robots_last_modified: Optional[str] = Field(None, description="Robots.txt last modified")
    allow_paths: List[str] = Field(default_factory=list, description="Allowed paths from robots.txt")
    disallow_paths: List[str] = Field(default_factory=list, description="Disallowed paths from robots.txt")
    crawl_delay: Optional[float] = Field(None, description="Crawl delay in seconds")
    start_ts: datetime = Field(..., description="Crawl start timestamp")
    end_ts: Optional[datetime] = Field(None, description="Crawl end timestamp")
    total_requests: int = Field(0, description="Total requests made")
    blocked_requests: int = Field(0, description="Number of blocked requests")
    rate_limit_violations: int = Field(0, description="Rate limit violations")


class RunManifest(BaseModel):
    """Run manifest for reproducibility."""
    run_id: str = Field(..., description="Unique run identifier")
    git_hash: Optional[str] = Field(None, description="Git commit hash")
    config_version: str = Field(..., description="Configuration version")
    start_ts: datetime = Field(..., description="Run start timestamp")
    end_ts: Optional[datetime] = Field(None, description="Run end timestamp")
    domains: List[str] = Field(..., description="Domains crawled")
    products_count: int = Field(0, description="Number of products scraped")
    reviews_count: int = Field(0, description="Number of reviews scraped")
    errors_count: int = Field(0, description="Number of errors encountered")
    compliance_manifests: List[ComplianceManifest] = Field(default_factory=list, description="Compliance manifests per domain")


class PriceStats(BaseModel):
    """Price statistics for luxury classification."""
    site: str = Field(..., description="Source site")
    category: str = Field(..., description="Product category")
    p25: float = Field(..., description="25th percentile price")
    p50: float = Field(..., description="50th percentile price")
    p75: float = Field(..., description="75th percentile price")
    p90: float = Field(..., description="90th percentile price")
    count: int = Field(..., description="Number of products in category")
    currency: str = Field(..., description="Price currency")
    computed_ts: datetime = Field(..., description="Computation timestamp")
