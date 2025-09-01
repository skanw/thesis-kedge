"""Sephora discovery module for finding products and pagination."""

import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs
import structlog
from ..common.parsing import SafeExtractor
from .selectors import get_selector_with_fallbacks, URL_PATTERNS

logger = structlog.get_logger(__name__)


class SephoraDiscovery:
    """Sephora product discovery and pagination handler."""
    
    def __init__(self, base_url: str = "https://www.sephora.fr"):
        self.base_url = base_url
        self.discovered_urls: Set[str] = set()
        self.seen_product_ids: Set[str] = set()
        
    def discover_products_from_category(self, category_url: str, 
                                      luxury_brands: List[str] = None,
                                      max_pages: int = 50) -> List[str]:
        """Discover product URLs from a category page."""
        try:
            logger.info("Starting product discovery", category_url=category_url)
            
            product_urls = []
            current_url = category_url
            page_count = 0
            
            while current_url and page_count < max_pages:
                logger.info("Discovering products from page", 
                           url=current_url, 
                           page=page_count + 1,
                           max_pages=max_pages)
                
                # In a real implementation, this would fetch the page HTML
                # For now, we'll simulate the discovery process
                
                # Extract product URLs from current page
                page_products = self._extract_product_urls_from_page(current_url)
                
                # Filter by luxury brands if specified
                if luxury_brands:
                    page_products = self._filter_by_brands(page_products, luxury_brands)
                
                # Add new products
                new_products = [url for url in page_products if url not in self.discovered_urls]
                product_urls.extend(new_products)
                self.discovered_urls.update(new_products)
                
                logger.info("Products found on page", 
                           page=page_count + 1,
                           total_products=len(page_products),
                           new_products=len(new_products),
                           cumulative_total=len(product_urls))
                
                # Get next page URL
                current_url = self._get_next_page_url(current_url)
                page_count += 1
                
                # Stop if no new products found
                if not new_products:
                    logger.info("No new products found, stopping discovery")
                    break
            
            logger.info("Product discovery completed", 
                       category_url=category_url,
                       total_pages=page_count,
                       total_products=len(product_urls))
            
            return product_urls
            
        except Exception as e:
            logger.error("Failed to discover products", category_url=category_url, error=str(e))
            return []
    
    def discover_refillable_products(self, category_url: str, 
                                   max_pages: int = 20) -> List[str]:
        """Discover refillable products using the refillable facet."""
        try:
            logger.info("Starting refillable product discovery", category_url=category_url)
            
            # Add refillable facet to URL
            refillable_url = self._add_refillable_facet(category_url)
            
            return self.discover_products_from_category(refillable_url, max_pages=max_pages)
            
        except Exception as e:
            logger.error("Failed to discover refillable products", category_url=category_url, error=str(e))
            return []
    
    def _extract_product_urls_from_page(self, page_url: str) -> List[str]:
        """Extract product URLs from a category page."""
        # In a real implementation, this would:
        # 1. Fetch the page HTML
        # 2. Use SafeExtractor to find product links
        # 3. Return the list of product URLs
        
        # For demonstration, we'll return a simulated list
        # In practice, this would use the selectors to find actual product links
        
        extractor = None  # Would be initialized with actual HTML
        
        if extractor:
            product_links = extractor.extract_list(
                get_selector_with_fallbacks("category", "product_link")["primary"],
                get_selector_with_fallbacks("category", "product_link")["fallback"]
            )
            
            # Convert relative URLs to absolute URLs
            product_urls = []
            for link in product_links:
                if link and '/product/' in link:
                    absolute_url = urljoin(self.base_url, link)
                    product_urls.append(absolute_url)
            
            return product_urls
        
        # Simulated return for demonstration
        return []
    
    def _filter_by_brands(self, product_urls: List[str], luxury_brands: List[str]) -> List[str]:
        """Filter product URLs by luxury brands."""
        # In a real implementation, this would:
        # 1. Extract brand information from product URLs or page content
        # 2. Filter based on the luxury brands list
        
        # For demonstration, we'll return all URLs
        # In practice, this would check the brand information
        
        filtered_urls = []
        for url in product_urls:
            # Extract brand from URL or would check page content
            brand = self._extract_brand_from_url(url)
            if brand and any(luxury_brand.lower() in brand.lower() for luxury_brand in luxury_brands):
                filtered_urls.append(url)
        
        return filtered_urls
    
    def _extract_brand_from_url(self, url: str) -> Optional[str]:
        """Extract brand name from product URL."""
        # Try to extract brand from URL pattern
        # Example: /product/chanel-n5-eau-de-parfum/P123456.html
        url_match = re.search(r'/product/([^/]+)-', url)
        if url_match:
            brand_part = url_match.group(1)
            # Convert URL format to brand name
            brand = brand_part.replace('-', ' ').title()
            return brand
        
        return None
    
    def _get_next_page_url(self, current_url: str) -> Optional[str]:
        """Get the next page URL for pagination."""
        # In a real implementation, this would:
        # 1. Parse the current page HTML
        # 2. Find the "next page" link
        # 3. Return the next page URL
        
        # For demonstration, we'll simulate pagination
        # In practice, this would use the pagination selectors
        
        # Check if there's a page parameter
        parsed_url = urlparse(current_url)
        query_params = parse_qs(parsed_url.query)
        
        current_page = int(query_params.get('page', [1])[0])
        next_page = current_page + 1
        
        # Add or update page parameter
        if 'page' in query_params:
            # Replace existing page parameter
            new_query = current_url.replace(f'page={current_page}', f'page={next_page}')
        else:
            # Add page parameter
            separator = '&' if '?' in current_url else '?'
            new_query = f"{current_url}{separator}page={next_page}"
        
        # In practice, we'd check if the next page actually exists
        # For now, we'll limit to 10 pages
        if next_page <= 10:
            return new_query
        
        return None
    
    def _add_refillable_facet(self, category_url: str) -> str:
        """Add refillable facet to category URL."""
        # Add refillable filter to URL
        separator = '&' if '?' in category_url else '?'
        return f"{category_url}{separator}facet=rechargeable"
    
    def get_discovered_count(self) -> int:
        """Get the number of discovered product URLs."""
        return len(self.discovered_urls)
    
    def reset_discovery(self):
        """Reset the discovery state."""
        self.discovered_urls.clear()
        self.seen_product_ids.clear()
    
    def is_url_discovered(self, url: str) -> bool:
        """Check if a URL has been discovered."""
        return url in self.discovered_urls
    
    def get_category_urls(self) -> List[str]:
        """Get the main category URLs for Sephora."""
        return [
            f"{self.base_url}/parfums",
            f"{self.base_url}/soins-visage",
            f"{self.base_url}/maquillage"
        ]
    
    def get_brand_filter_urls(self, luxury_brands: List[str]) -> List[str]:
        """Get category URLs filtered by luxury brands."""
        category_urls = self.get_category_urls()
        brand_urls = []
        
        for category_url in category_urls:
            for brand in luxury_brands:
                # Create brand-filtered URL
                brand_param = brand.lower().replace(' ', '-')
                separator = '&' if '?' in category_url else '?'
                brand_url = f"{category_url}{separator}brand={brand_param}"
                brand_urls.append(brand_url)
        
        return brand_urls
