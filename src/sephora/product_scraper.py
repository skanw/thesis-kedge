"""Sephora product scraper for extracting product details and refillable detection."""

import re
from datetime import datetime
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
import structlog
from ..common.parsing import SafeExtractor, TextNormalizer, RefillableDetector, LanguageDetector
from ..common.schema import Product, RefillEvidence
from .selectors import get_selector_with_fallbacks

logger = structlog.get_logger(__name__)


class SephoraProductScraper:
    """Sephora product scraper with refillable detection."""
    
    def __init__(self, refillable_keywords: Dict[str, List[str]]):
        self.refillable_detector = RefillableDetector(refillable_keywords)
        self.text_normalizer = TextNormalizer()
        self.language_detector = LanguageDetector()
        
    def scrape_product(self, html: str, url: str, site: str = "sephora") -> Optional[Product]:
        """Scrape product details from HTML."""
        try:
            extractor = SafeExtractor(html, url)
            
            # Extract basic product information
            product_data = self._extract_basic_info(extractor, url, site)
            if not product_data:
                logger.warning("Failed to extract basic product info", url=url)
                return None
            
            # Extract additional details
            product_data.update(self._extract_additional_info(extractor))
            
            # Detect refillable status
            refillable_info = self._detect_refillable(extractor, product_data)
            product_data.update(refillable_info)
            
            # Normalize and validate data
            product_data = self._normalize_data(product_data)
            
            # Create Product object
            product = Product(**product_data)
            
            logger.info("Product scraped successfully", 
                       product_id=product.product_id,
                       brand=product.brand,
                       name=product.name,
                       refillable=product.refillable_flag)
            
            return product
            
        except Exception as e:
            logger.error("Failed to scrape product", url=url, error=str(e))
            return None
    
    def _extract_basic_info(self, extractor: SafeExtractor, url: str, site: str) -> Optional[Dict[str, Any]]:
        """Extract basic product information."""
        # Product ID
        product_id = self._extract_product_id(extractor, url)
        if not product_id:
            return None
        
        # Brand
        brand = extractor.extract_text(
            get_selector_with_fallbacks("product", "brand")["primary"],
            get_selector_with_fallbacks("product", "brand")["fallback"]
        )
        if not brand:
            logger.warning("No brand found", url=url)
            return None
        
        # Product name
        name = extractor.extract_text(
            get_selector_with_fallbacks("product", "name")["primary"],
            get_selector_with_fallbacks("product", "name")["fallback"]
        )
        if not name:
            logger.warning("No product name found", url=url)
            return None
        
        # Price
        price_value = extractor.extract_price(
            get_selector_with_fallbacks("product", "price")["primary"],
            get_selector_with_fallbacks("product", "price")["fallback"]
        )
        if not price_value:
            logger.warning("No price found", url=url)
            return None
        
        # Currency (default to EUR for Sephora France)
        currency = extractor.extract_text(
            get_selector_with_fallbacks("product", "currency")["primary"],
            get_selector_with_fallbacks("product", "currency")["fallback"]
        ) or "EUR"
        
        return {
            "product_id": product_id,
            "site": site,
            "url": url,
            "brand": brand,
            "name": name,
            "price_value": price_value,
            "price_currency": currency,
            "scrape_ts": datetime.now(),
            "first_seen_ts": datetime.now(),
            "last_seen_ts": datetime.now(),
            "source_site": urlparse(url).netloc,
            "source_url": url
        }
    
    def _extract_additional_info(self, extractor: SafeExtractor) -> Dict[str, Any]:
        """Extract additional product information."""
        # Size
        size = extractor.extract_text(
            get_selector_with_fallbacks("product", "size")["primary"],
            get_selector_with_fallbacks("product", "size")["fallback"]
        )
        size_ml_or_g = extractor.extract_size(
            get_selector_with_fallbacks("product", "size")["primary"],
            get_selector_with_fallbacks("product", "size")["fallback"]
        )
        
        # Rating
        rating_avg = extractor.extract_rating(
            get_selector_with_fallbacks("product", "rating_avg")["primary"],
            get_selector_with_fallbacks("product", "rating_avg")["fallback"]
        )
        
        rating_count_text = extractor.extract_text(
            get_selector_with_fallbacks("product", "rating_count")["primary"],
            get_selector_with_fallbacks("product", "rating_count")["fallback"]
        )
        rating_count = self._extract_number_from_text(rating_count_text) if rating_count_text else None
        
        # Availability
        availability = extractor.extract_text(
            get_selector_with_fallbacks("product", "availability")["primary"],
            get_selector_with_fallbacks("product", "availability")["fallback"]
        )
        
        # Image URL
        image_url = extractor.extract_image_url(
            get_selector_with_fallbacks("product", "image_url")["primary"],
            get_selector_with_fallbacks("product", "image_url")["fallback"]
        )
        
        # Canonical URL
        canonical_url = extractor.extract_url(
            get_selector_with_fallbacks("product", "canonical_url")["primary"],
            get_selector_with_fallbacks("product", "canonical_url")["fallback"]
        )
        
        # Breadcrumbs
        breadcrumb_elements = extractor.extract_list(
            get_selector_with_fallbacks("product", "breadcrumbs")["primary"],
            get_selector_with_fallbacks("product", "breadcrumbs")["fallback"]
        )
        breadcrumbs = self.text_normalizer.extract_breadcrumbs(breadcrumb_elements)
        
        # EAN/GTIN
        ean_gtin = extractor.extract_text(
            get_selector_with_fallbacks("product", "ean_gtin")["primary"],
            get_selector_with_fallbacks("product", "ean_gtin")["fallback"]
        )
        
        # Ingredients
        ingredients_text = extractor.extract_text(
            get_selector_with_fallbacks("product", "ingredients")["primary"],
            get_selector_with_fallbacks("product", "ingredients")["fallback"]
        )
        ingredients_present = bool(ingredients_text)
        
        # Product line
        line = extractor.extract_text(
            get_selector_with_fallbacks("product", "line")["primary"],
            get_selector_with_fallbacks("product", "line")["fallback"]
        )
        
        # Category path (from breadcrumbs)
        category_path = self._extract_category_path(breadcrumbs)
        
        return {
            "size": size,
            "size_ml_or_g": size_ml_or_g,
            "rating_avg": rating_avg,
            "rating_count": rating_count,
            "availability": availability,
            "image_url": image_url,
            "canonical_url": canonical_url,
            "breadcrumbs": breadcrumbs,
            "category_path": category_path,
            "ean_gtin": ean_gtin,
            "ingredients_present": ingredients_present,
            "line": line,
            "packaging_notes": None,  # Could be extracted if available
            "refill_parent_sku": None  # Could be detected if available
        }
    
    def _detect_refillable(self, extractor: SafeExtractor, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect if product is refillable with evidence."""
        # Extract refillable badges
        refillable_badges = extractor.extract_list(
            get_selector_with_fallbacks("product", "refillable_badges")["primary"],
            get_selector_with_fallbacks("product", "refillable_badges")["fallback"]
        )
        
        # Combine all text content for keyword detection
        text_content = " ".join([
            product_data.get("name", ""),
            product_data.get("line", ""),
            " ".join(product_data.get("breadcrumbs", [])),
            " ".join(refillable_badges)
        ])
        
        # Detect refillable status
        is_refillable, evidence = self.refillable_detector.detect_refillable(
            text_content=text_content,
            badges=refillable_badges
        )
        
        return {
            "refillable_flag": is_refillable,
            "refill_evidence": evidence
        }
    
    def _extract_product_id(self, extractor: SafeExtractor, url: str) -> Optional[str]:
        """Extract product ID from URL or page content."""
        # Try to extract from URL first
        url_match = re.search(r'/product/([^/?]+)', url)
        if url_match:
            return url_match.group(1)
        
        # Try to extract from page content
        product_id = extractor.extract_text(
            get_selector_with_fallbacks("product", "product_id")["primary"],
            get_selector_with_fallbacks("product", "product_id")["fallback"]
        )
        
        if product_id:
            # Clean up the product ID
            product_id = re.sub(r'[^\w\-]', '', product_id)
            return product_id
        
        return None
    
    def _extract_number_from_text(self, text: str) -> Optional[int]:
        """Extract number from text."""
        if not text:
            return None
        
        # Remove non-numeric characters except digits
        number_match = re.search(r'(\d+)', text.replace(',', '').replace('.', ''))
        if number_match:
            try:
                return int(number_match.group(1))
            except ValueError:
                pass
        
        return None
    
    def _extract_category_path(self, breadcrumbs: List[str]) -> List[str]:
        """Extract category path from breadcrumbs."""
        if not breadcrumbs:
            return ["unknown"]
        
        # Filter out common non-category breadcrumbs
        category_keywords = ["parfums", "soins", "maquillage", "visage", "corps", "yeux", "lèvres"]
        categories = []
        
        for breadcrumb in breadcrumbs:
            breadcrumb_lower = breadcrumb.lower()
            if any(keyword in breadcrumb_lower for keyword in category_keywords):
                normalized = self.text_normalizer.normalize_category(breadcrumb)
                if normalized:
                    categories.append(normalized)
        
        return categories if categories else ["unknown"]
    
    def _normalize_data(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize and clean product data."""
        # Normalize brand
        if product_data.get("brand"):
            product_data["brand"] = self.text_normalizer.normalize_brand(product_data["brand"])
        
        # Ensure required fields have default values
        product_data.setdefault("refillable_flag", False)
        product_data.setdefault("refill_evidence", [])
        product_data.setdefault("category_path", ["unknown"])
        product_data.setdefault("breadcrumbs", [])
        
        return product_data
