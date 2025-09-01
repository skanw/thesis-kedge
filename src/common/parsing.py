"""HTML parsing utilities with safe extractors and fallback strategies."""

import re
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urljoin, urlparse
from selectolax.parser import HTMLParser
import structlog

logger = structlog.get_logger(__name__)


class SafeExtractor:
    """Safe HTML content extractor with multiple fallback strategies."""
    
    def __init__(self, html: str, base_url: str = ""):
        self.html = html
        self.base_url = base_url
        self.parser = HTMLParser(html)
        
    def extract_text(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> Optional[str]:
        """Extract text using multiple selector strategies."""
        # Try primary selectors
        for selector in selectors:
            try:
                element = self.parser.css_first(selector)
                if element and element.text():
                    text = element.text().strip()
                    if text:
                        return text
            except Exception as e:
                logger.debug("Selector failed", selector=selector, error=str(e))
                continue
        
        # Try fallback selectors
        if fallback_selectors:
            for selector in fallback_selectors:
                try:
                    element = self.parser.css_first(selector)
                    if element and element.text():
                        text = element.text().strip()
                        if text:
                            return text
                except Exception as e:
                    logger.debug("Fallback selector failed", selector=selector, error=str(e))
                    continue
        
        return None
    
    def extract_attribute(self, selectors: List[str], attribute: str, 
                         fallback_selectors: Optional[List[str]] = None) -> Optional[str]:
        """Extract attribute value using multiple selector strategies."""
        # Try primary selectors
        for selector in selectors:
            try:
                element = self.parser.css_first(selector)
                if element and element.attributes.get(attribute):
                    value = element.attributes[attribute].strip()
                    if value:
                        return value
            except Exception as e:
                logger.debug("Attribute selector failed", selector=selector, attribute=attribute, error=str(e))
                continue
        
        # Try fallback selectors
        if fallback_selectors:
            for selector in fallback_selectors:
                try:
                    element = self.parser.css_first(selector)
                    if element and element.attributes.get(attribute):
                        value = element.attributes[attribute].strip()
                        if value:
                            return value
                except Exception as e:
                    logger.debug("Fallback attribute selector failed", selector=selector, attribute=attribute, error=str(e))
                    continue
        
        return None
    
    def extract_url(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> Optional[str]:
        """Extract and normalize URL."""
        url = self.extract_attribute(selectors, "href", fallback_selectors)
        if url and self.base_url:
            return urljoin(self.base_url, url)
        return url
    
    def extract_image_url(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> Optional[str]:
        """Extract and normalize image URL."""
        url = self.extract_attribute(selectors, "src", fallback_selectors)
        if url and self.base_url:
            return urljoin(self.base_url, url)
        return url
    
    def extract_list(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> List[str]:
        """Extract list of text values."""
        results = []
        
        # Try primary selectors
        for selector in selectors:
            try:
                elements = self.parser.css(selector)
                for element in elements:
                    if element and element.text():
                        text = element.text().strip()
                        if text:
                            results.append(text)
                if results:
                    return results
            except Exception as e:
                logger.debug("List selector failed", selector=selector, error=str(e))
                continue
        
        # Try fallback selectors
        if fallback_selectors:
            for selector in fallback_selectors:
                try:
                    elements = self.parser.css(selector)
                    for element in elements:
                        if element and element.text():
                            text = element.text().strip()
                            if text:
                                results.append(text)
                    if results:
                        return results
                except Exception as e:
                    logger.debug("Fallback list selector failed", selector=selector, error=str(e))
                    continue
        
        return results
    
    def extract_rating(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> Optional[float]:
        """Extract rating value from various formats."""
        rating_text = self.extract_text(selectors, fallback_selectors)
        if not rating_text:
            return None
        
        # Try to extract numeric rating
        rating_patterns = [
            r'(\d+(?:\.\d+)?)/5',  # 4.5/5
            r'(\d+(?:\.\d+)?)\s*étoiles?',  # 4.5 étoiles
            r'(\d+(?:\.\d+)?)\s*stars?',  # 4.5 stars
            r'(\d+(?:\.\d+)?)',  # Just the number
        ]
        
        for pattern in rating_patterns:
            match = re.search(pattern, rating_text, re.IGNORECASE)
            if match:
                try:
                    rating = float(match.group(1))
                    if 0 <= rating <= 5:
                        return rating
                except ValueError:
                    continue
        
        return None
    
    def extract_price(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> Optional[float]:
        """Extract price value from various formats."""
        price_text = self.extract_text(selectors, fallback_selectors)
        if not price_text:
            return None
        
        # Remove currency symbols and spaces
        price_clean = re.sub(r'[^\d,.]', '', price_text)
        
        # Handle different decimal separators
        if ',' in price_clean and '.' in price_clean:
            # European format: 1.234,56
            price_clean = price_clean.replace('.', '').replace(',', '.')
        elif ',' in price_clean:
            # Could be either format, assume comma is decimal separator
            price_clean = price_clean.replace(',', '.')
        
        try:
            price = float(price_clean)
            return price if price > 0 else None
        except ValueError:
            return None
    
    def extract_size(self, selectors: List[str], fallback_selectors: Optional[List[str]] = None) -> Optional[float]:
        """Extract size in ml or grams."""
        size_text = self.extract_text(selectors, fallback_selectors)
        if not size_text:
            return None
        
        # Extract numeric value with unit
        size_patterns = [
            r'(\d+(?:\.\d+)?)\s*ml',  # 100 ml
            r'(\d+(?:\.\d+)?)\s*g',   # 50 g
            r'(\d+(?:\.\d+)?)\s*grammes?',  # 50 grammes
            r'(\d+(?:\.\d+)?)',  # Just the number
        ]
        
        for pattern in size_patterns:
            match = re.search(pattern, size_text, re.IGNORECASE)
            if match:
                try:
                    size = float(match.group(1))
                    return size if size > 0 else None
                except ValueError:
                    continue
        
        return None


class TextNormalizer:
    """Text normalization utilities."""
    
    @staticmethod
    def normalize_brand(brand_text: str) -> str:
        """Normalize brand name."""
        if not brand_text:
            return ""
        
        # Remove common prefixes/suffixes
        brand = brand_text.strip()
        brand = re.sub(r'^(Parfums?\s+|Maison\s+)', '', brand, flags=re.IGNORECASE)
        brand = re.sub(r'\s+(Beauty|Parfums?|Cosmetics)$', '', brand, flags=re.IGNORECASE)
        
        return brand.strip()
    
    @staticmethod
    def normalize_category(category_text: str) -> str:
        """Normalize category name."""
        if not category_text:
            return ""
        
        category = category_text.strip().lower()
        
        # Common French to English mappings
        mappings = {
            'parfums': 'fragrance',
            'soins': 'skincare',
            'maquillage': 'makeup',
            'visage': 'face',
            'corps': 'body',
            'cheveux': 'hair',
            'yeux': 'eyes',
            'lèvres': 'lips',
        }
        
        for french, english in mappings.items():
            category = category.replace(french, english)
        
        return category.strip()
    
    @staticmethod
    def extract_breadcrumbs(breadcrumb_elements: List[str]) -> List[str]:
        """Extract and normalize breadcrumbs."""
        breadcrumbs = []
        for element in breadcrumb_elements:
            if element and element.strip():
                breadcrumbs.append(element.strip())
        return breadcrumbs


class RefillableDetector:
    """Detector for refillable/rechargeable products."""
    
    def __init__(self, keywords: Dict[str, List[str]]):
        self.keywords = keywords
    
    def detect_refillable(self, text_content: str, facets: List[str] = None, badges: List[str] = None) -> tuple[bool, List[str]]:
        """Detect if product is refillable with evidence."""
        evidence = []
        
        # Check facets first (highest priority)
        if facets:
            for facet in facets:
                if self._contains_refillable_keyword(facet):
                    evidence.append("facet")
                    return True, evidence
        
        # Check badges
        if badges:
            for badge in badges:
                if self._contains_refillable_keyword(badge):
                    evidence.append("badge")
                    return True, evidence
        
        # Check text content
        if text_content and self._contains_refillable_keyword(text_content):
            evidence.append("attribute_text")
            return True, evidence
        
        return False, evidence
    
    def _contains_refillable_keyword(self, text: str) -> bool:
        """Check if text contains refillable keywords."""
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Check French keywords
        for keyword in self.keywords.get('french', []):
            if keyword.lower() in text_lower:
                return True
        
        # Check English keywords
        for keyword in self.keywords.get('english', []):
            if keyword.lower() in text_lower:
                return True
        
        return False


class LanguageDetector:
    """Language detection for reviews."""
    
    @staticmethod
    def detect_language(text: str) -> str:
        """Detect language of text."""
        if not text:
            return "other"
        
        # Simple French detection
        french_indicators = [
            'le', 'la', 'les', 'un', 'une', 'des', 'et', 'ou', 'avec', 'sans',
            'très', 'plus', 'moins', 'bon', 'bonne', 'mauvais', 'mauvaise',
            'parfait', 'parfaite', 'excellent', 'excellente', 'super', 'génial'
        ]
        
        text_lower = text.lower()
        french_count = sum(1 for word in french_indicators if word in text_lower)
        
        # Simple English detection
        english_indicators = [
            'the', 'a', 'an', 'and', 'or', 'with', 'without', 'very', 'more',
            'less', 'good', 'bad', 'perfect', 'excellent', 'great', 'amazing'
        ]
        
        english_count = sum(1 for word in english_indicators if word in text_lower)
        
        if french_count > english_count:
            return "fr"
        elif english_count > french_count:
            return "en"
        else:
            return "other"
