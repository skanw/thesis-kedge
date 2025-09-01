"""Sephora-specific CSS/XPath selectors for product and review extraction."""

from typing import Dict, List, Any

# Product page selectors
PRODUCT_SELECTORS = {
    # Product ID/SKU
    "product_id": [
        "[data-product-id]",
        ".product-id",
        "[data-sku]",
        ".sku"
    ],
    
    # Brand
    "brand": [
        ".product-brand",
        "[data-brand]",
        ".brand-name",
        "h1 .brand"
    ],
    
    # Product name
    "name": [
        ".product-name",
        "h1 .product-title",
        ".product-title",
        "[data-product-name]"
    ],
    
    # Price
    "price": [
        ".product-price",
        ".price-current",
        "[data-price]",
        ".price"
    ],
    
    # Currency
    "currency": [
        ".price-currency",
        ".currency",
        "[data-currency]"
    ],
    
    # Size
    "size": [
        ".product-size",
        ".size",
        "[data-size]",
        ".capacity"
    ],
    
    # Rating
    "rating_avg": [
        ".rating-average",
        ".stars-rating",
        "[data-rating]",
        ".product-rating"
    ],
    
    "rating_count": [
        ".rating-count",
        ".reviews-count",
        "[data-review-count]",
        ".number-of-reviews"
    ],
    
    # Availability
    "availability": [
        ".product-availability",
        ".availability",
        "[data-availability]",
        ".stock-status"
    ],
    
    # Image
    "image_url": [
        ".product-image img",
        ".main-image img",
        "[data-image] img",
        ".product-photo img"
    ],
    
    # Breadcrumbs
    "breadcrumbs": [
        ".breadcrumb li",
        ".breadcrumbs li",
        ".navigation-path li",
        "nav[aria-label='breadcrumb'] li"
    ],
    
    # EAN/GTIN
    "ean_gtin": [
        "[data-ean]",
        "[data-gtin]",
        ".product-code",
        ".barcode"
    ],
    
    # Ingredients
    "ingredients": [
        ".ingredients",
        ".product-ingredients",
        "[data-ingredients]",
        ".composition"
    ],
    
    # Refillable badges
    "refillable_badges": [
        ".refillable-badge",
        ".rechargeable-badge",
        "[data-refillable]",
        ".eco-badge"
    ],
    
    # Product line
    "line": [
        ".product-line",
        ".collection",
        "[data-line]",
        ".series"
    ],
    
    # Canonical URL
    "canonical_url": [
        "link[rel='canonical']",
        "[data-canonical]"
    ]
}

# Review page selectors
REVIEW_SELECTORS = {
    # Review container
    "review_container": [
        ".review-item",
        ".review",
        "[data-review]",
        ".customer-review"
    ],
    
    # Review ID
    "review_id": [
        "[data-review-id]",
        ".review-id",
        "[data-id]"
    ],
    
    # Rating
    "rating": [
        ".review-rating",
        ".stars",
        "[data-rating]",
        ".rating"
    ],
    
    # Title
    "title": [
        ".review-title",
        ".title",
        ".review-headline",
        "h3"
    ],
    
    # Body
    "body": [
        ".review-text",
        ".review-content",
        ".review-body",
        ".content"
    ],
    
    # Date
    "date": [
        ".review-date",
        ".date",
        "[data-date]",
        ".timestamp"
    ],
    
    # Author
    "author": [
        ".review-author",
        ".author",
        ".reviewer",
        ".customer-name"
    ],
    
    # Verified purchase
    "verified_purchase": [
        ".verified-purchase",
        ".verified",
        "[data-verified]",
        ".purchase-verified"
    ],
    
    # Helpful count
    "helpful_count": [
        ".helpful-count",
        ".votes",
        "[data-helpful]",
        ".useful-count"
    ]
}

# Category/facet selectors
CATEGORY_SELECTORS = {
    # Product grid
    "product_grid": [
        ".product-grid",
        ".products-list",
        ".product-cards",
        "[data-products]"
    ],
    
    # Product card
    "product_card": [
        ".product-card",
        ".product-item",
        ".product",
        "[data-product]"
    ],
    
    # Product link
    "product_link": [
        ".product-link",
        "a[href*='/product/']",
        ".product-url",
        "a.product-card"
    ],
    
    # Pagination
    "pagination": [
        ".pagination",
        ".page-navigation",
        ".pager",
        "[data-pagination]"
    ],
    
    # Next page
    "next_page": [
        ".next-page",
        ".pagination-next",
        "a[rel='next']",
        ".next"
    ],
    
    # Facets/filters
    "facets": [
        ".facets",
        ".filters",
        ".filter-options",
        "[data-facets]"
    ],
    
    # Refillable facet
    "refillable_facet": [
        "[data-facet='rechargeable']",
        ".facet-rechargeable",
        "[data-filter='refillable']",
        ".eco-filter"
    ],
    
    # Brand filters
    "brand_filters": [
        ".brand-filter",
        ".brand-facet",
        "[data-brand-filter]",
        ".brand-options"
    ]
}

# Fallback selectors for robustness
FALLBACK_SELECTORS = {
    "product_id": [
        "script[type='application/ld+json']",
        "[data-product-sku]",
        ".product-sku"
    ],
    
    "brand": [
        "script[type='application/ld+json']",
        ".product-info .brand",
        "h1 span:first-child"
    ],
    
    "name": [
        "script[type='application/ld+json']",
        "h1",
        ".product-info h1",
        "title"
    ],
    
    "price": [
        "script[type='application/ld+json']",
        ".price span",
        ".product-price span",
        "[class*='price']"
    ],
    
    "rating_avg": [
        "script[type='application/ld+json']",
        ".rating span",
        "[class*='rating']",
        ".stars"
    ],
    
    "review_container": [
        ".review",
        "[class*='review']",
        ".comment",
        ".feedback"
    ],
    
    "body": [
        ".review p",
        ".content p",
        ".text",
        ".description"
    ]
}

# XHR/API endpoints for reviews
REVIEW_API_ENDPOINTS = {
    "reviews_api": "/api/reviews",
    "reviews_xhr": "/reviews/ajax",
    "product_reviews": "/api/product/{product_id}/reviews"
}

# URL patterns
URL_PATTERNS = {
    "product": r"/product/([^/]+)",
    "category": r"/(parfums|soins|maquillage)",
    "reviews": r"/reviews",
    "refillable": r"rechargeable|refill"
}

# Language detection patterns
LANGUAGE_PATTERNS = {
    "french": [
        "étoiles",
        "avis",
        "client",
        "produit",
        "parfum",
        "soin",
        "maquillage"
    ],
    "english": [
        "stars",
        "review",
        "customer",
        "product",
        "fragrance",
        "skincare",
        "makeup"
    ]
}

def get_selector_with_fallbacks(selector_type: str, field: str) -> Dict[str, List[str]]:
    """Get selectors with fallbacks for a specific field."""
    selectors = {}
    
    if selector_type == "product":
        selectors["primary"] = PRODUCT_SELECTORS.get(field, [])
        selectors["fallback"] = FALLBACK_SELECTORS.get(field, [])
    elif selector_type == "review":
        selectors["primary"] = REVIEW_SELECTORS.get(field, [])
        selectors["fallback"] = FALLBACK_SELECTORS.get(field, [])
    elif selector_type == "category":
        selectors["primary"] = CATEGORY_SELECTORS.get(field, [])
        selectors["fallback"] = []
    
    return selectors

def get_all_selectors() -> Dict[str, Any]:
    """Get all selectors organized by type."""
    return {
        "product": PRODUCT_SELECTORS,
        "review": REVIEW_SELECTORS,
        "category": CATEGORY_SELECTORS,
        "fallback": FALLBACK_SELECTORS,
        "api": REVIEW_API_ENDPOINTS,
        "url_patterns": URL_PATTERNS,
        "language_patterns": LANGUAGE_PATTERNS
    }
