"""Sephora reviews scraper for extracting review data with language detection."""

import re
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Set
from urllib.parse import urljoin, urlparse
import structlog
from ..common.parsing import SafeExtractor, LanguageDetector
from ..common.schema import Review, Language
from ..common.utils import TimestampManager
from .selectors import get_selector_with_fallbacks

logger = structlog.get_logger(__name__)


class SephoraReviewsScraper:
    """Sephora reviews scraper with language detection."""
    
    def __init__(self, preferred_language: str = "fr"):
        self.language_detector = LanguageDetector()
        self.preferred_language = preferred_language
        self.seen_review_ids: Set[str] = set()
        
    def scrape_reviews(self, html: str, product_url: str, product_id: str, 
                      site: str = "sephora") -> List[Review]:
        """Scrape reviews from product page HTML."""
        try:
            extractor = SafeExtractor(html, product_url)
            
            # Extract review containers
            review_containers = self._extract_review_containers(extractor)
            if not review_containers:
                logger.info("No reviews found", product_url=product_url)
                return []
            
            reviews = []
            for container in review_containers:
                review = self._extract_single_review(container, product_url, product_id, site)
                if review and review.review_id not in self.seen_review_ids:
                    reviews.append(review)
                    self.seen_review_ids.add(review.review_id)
            
            logger.info("Reviews scraped", 
                       product_url=product_url,
                       total_reviews=len(review_containers),
                       new_reviews=len(reviews))
            
            return reviews
            
        except Exception as e:
            logger.error("Failed to scrape reviews", product_url=product_url, error=str(e))
            return []
    
    def _extract_review_containers(self, extractor: SafeExtractor) -> List[str]:
        """Extract review container HTML elements."""
        # Try to find review containers
        review_selectors = get_selector_with_fallbacks("review", "review_container")
        
        # For now, we'll use a simple approach to find review sections
        # In a real implementation, this would use the selectors to find actual review containers
        # For demonstration, we'll return an empty list
        return []
    
    def _extract_single_review(self, review_html: str, product_url: str, 
                              product_id: str, site: str) -> Optional[Review]:
        """Extract a single review from HTML."""
        try:
            extractor = SafeExtractor(review_html, product_url)
            
            # Extract review ID
            review_id = self._extract_review_id(extractor, product_id)
            if not review_id:
                return None
            
            # Extract rating
            rating = self._extract_rating(extractor)
            if not rating:
                return None
            
            # Extract body text
            body = self._extract_body(extractor)
            if not body:
                return None
            
            # Extract additional fields
            title = extractor.extract_text(
                get_selector_with_fallbacks("review", "title")["primary"],
                get_selector_with_fallbacks("review", "title")["fallback"]
            )
            
            review_date = self._extract_date(extractor)
            if not review_date:
                review_date = date.today()  # Default to today if no date found
            
            # Extract author information (only anonymized labels)
            author_label = self._extract_author_label(extractor)
            
            # Extract verified purchase status
            verified_purchase = self._extract_verified_purchase(extractor)
            
            # Extract helpful count
            helpful_count = self._extract_helpful_count(extractor)
            
            # Detect language
            language = self._detect_language(body, title)
            
            # Create review URL
            review_url = self._create_review_url(product_url, review_id)
            
            # Create Review object
            review = Review(
                review_id=review_id,
                product_id=product_id,
                site=site,
                url=review_url,
                rating=rating,
                title=title,
                body=body,
                language=language,
                review_date=review_date,
                verified_purchase=verified_purchase,
                helpful_count=helpful_count,
                author_label=author_label,
                scrape_ts=datetime.now()
            )
            
            return review
            
        except Exception as e:
            logger.error("Failed to extract single review", error=str(e))
            return None
    
    def _extract_review_id(self, extractor: SafeExtractor, product_id: str) -> Optional[str]:
        """Extract review ID."""
        review_id = extractor.extract_text(
            get_selector_with_fallbacks("review", "review_id")["primary"],
            get_selector_with_fallbacks("review", "review_id")["fallback"]
        )
        
        if review_id:
            return review_id
        
        # Fallback: generate ID from product and timestamp
        return f"{product_id}_review_{int(datetime.now().timestamp())}"
    
    def _extract_rating(self, extractor: SafeExtractor) -> Optional[int]:
        """Extract rating value."""
        rating = extractor.extract_rating(
            get_selector_with_fallbacks("review", "rating")["primary"],
            get_selector_with_fallbacks("review", "rating")["fallback"]
        )
        
        if rating and 1 <= rating <= 5:
            return int(rating)
        
        return None
    
    def _extract_body(self, extractor: SafeExtractor) -> Optional[str]:
        """Extract review body text."""
        body = extractor.extract_text(
            get_selector_with_fallbacks("review", "body")["primary"],
            get_selector_with_fallbacks("review", "body")["fallback"]
        )
        
        if body:
            # Clean up the text
            body = body.strip()
            # Remove excessive whitespace
            body = re.sub(r'\s+', ' ', body)
            return body if len(body) > 10 else None  # Minimum length check
        
        return None
    
    def _extract_date(self, extractor: SafeExtractor) -> Optional[date]:
        """Extract review date."""
        date_text = extractor.extract_text(
            get_selector_with_fallbacks("review", "date")["primary"],
            get_selector_with_fallbacks("review", "date")["fallback"]
        )
        
        if not date_text:
            return None
        
        # Try to parse various date formats
        date_formats = [
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d %B %Y",  # French format: 15 janvier 2024
            "%B %d, %Y",  # English format: January 15, 2024
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_text.strip(), fmt).date()
            except ValueError:
                continue
        
        # Try to extract date from text using regex
        date_patterns = [
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            r'(\d{1,2})-(\d{1,2})-(\d{4})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                try:
                    if len(match.groups()) == 3:
                        if len(match.group(1)) == 4:  # YYYY-MM-DD
                            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
                        else:  # DD/MM/YYYY or DD-MM-YYYY
                            return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
                except ValueError:
                    continue
        
        return None
    
    def _extract_author_label(self, extractor: SafeExtractor) -> Optional[str]:
        """Extract anonymized author label."""
        author = extractor.extract_text(
            get_selector_with_fallbacks("review", "author")["primary"],
            get_selector_with_fallbacks("review", "author")["fallback"]
        )
        
        if not author:
            return None
        
        # Only return if it's an anonymized label
        anonymized_patterns = [
            r'client\s+vérifié',
            r'verified\s+customer',
            r'client\s+anon',
            r'anonymous\s+customer',
            r'utilisateur',
            r'user'
        ]
        
        author_lower = author.lower()
        for pattern in anonymized_patterns:
            if re.search(pattern, author_lower):
                return author.strip()
        
        # Don't return personal information
        return None
    
    def _extract_verified_purchase(self, extractor: SafeExtractor) -> Optional[bool]:
        """Extract verified purchase status."""
        verified_text = extractor.extract_text(
            get_selector_with_fallbacks("review", "verified_purchase")["primary"],
            get_selector_with_fallbacks("review", "verified_purchase")["fallback"]
        )
        
        if not verified_text:
            return None
        
        verified_lower = verified_text.lower()
        if any(keyword in verified_lower for keyword in ['vérifié', 'verified', 'confirmé', 'confirmed']):
            return True
        elif any(keyword in verified_lower for keyword in ['non vérifié', 'not verified', 'non confirmé']):
            return False
        
        return None
    
    def _extract_helpful_count(self, extractor: SafeExtractor) -> Optional[int]:
        """Extract helpful vote count."""
        helpful_text = extractor.extract_text(
            get_selector_with_fallbacks("review", "helpful_count")["primary"],
            get_selector_with_fallbacks("review", "helpful_count")["fallback"]
        )
        
        if not helpful_text:
            return None
        
        # Extract number from text
        number_match = re.search(r'(\d+)', helpful_text.replace(',', '').replace('.', ''))
        if number_match:
            try:
                return int(number_match.group(1))
            except ValueError:
                pass
        
        return None
    
    def _detect_language(self, body: str, title: Optional[str] = None) -> Language:
        """Detect review language."""
        text = body
        if title:
            text = f"{title} {body}"
        
        detected = self.language_detector.detect_language(text)
        
        if detected == "fr":
            return Language.FRENCH
        elif detected == "en":
            return Language.ENGLISH
        else:
            return Language.OTHER
    
    def _create_review_url(self, product_url: str, review_id: str) -> str:
        """Create review URL."""
        # For Sephora, reviews are typically on the product page
        # We'll append a fragment to identify the specific review
        return f"{product_url}#review-{review_id}"
    
    def reset_seen_reviews(self):
        """Reset the set of seen review IDs."""
        self.seen_review_ids.clear()
    
    def get_seen_review_count(self) -> int:
        """Get the number of seen review IDs."""
        return len(self.seen_review_ids)
