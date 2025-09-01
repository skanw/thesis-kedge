# Thesis Kedge: Domain-Agnostic Web Data Platform

A **domain-agnostic, adapter-based web data platform** for compliant e-commerce data collection. Originally designed for French luxury beauty research, the system now supports **any website** through a clean adapter interface.

## 🚀 Key Features

- **🔌 Adapter Pattern**: Add new sites in <200 lines of code
- **🤖 Robots.txt Compliance**: Strict adherence to robots.txt and ethical crawling
- **📊 Comprehensive Metrics**: HTTP status tracking, selector failures, data quality
- **🔄 Conditional GET**: ETag/If-Modified-Since support for ethical bandwidth
- **🗺️ Sitemap Discovery**: High-yield URL discovery with low selector fragility
- **🛡️ Integrity Gates**: Synthetic data detection and provenance enforcement
- **📈 Observability**: Real-time metrics and quality monitoring
- **🧪 Golden Fixtures**: Selector testing with saved HTML samples

## Research Topic
Big-Data Customer Segmentation in French Luxury Beauty: clustering retail review behavior and "Refillable" sustainability signals.

## Quick Start

```bash
# Install dependencies
poetry install

# Crawl a real website with compliance
python -m src.cli crawl-real --site marionnaud.fr --max-pages 10 --rate-limit 0.5

# Validate data integrity
python -m src.cli validate

# Check metrics and quality
ls data/metrics/
```

## Adding New Sites

See [ADAPTER_SDK_README.md](ADAPTER_SDK_README.md) for a complete guide on adding new sites in <200 lines of code.

```python
# Example: Add support for a new site
class YourSiteAdapter(BaseAdapter):
    def __init__(self):
        super().__init__("yoursite.com", "Your Site")
    
    def discovery(self, ctx: CrawlContext) -> List[ProductRef]:
        # Discover product URLs
        return []
    
    def fetch_product(self, ref: ProductRef, ctx: CrawlContext) -> Optional[Product]:
        # Extract product data
        return None
```

## Architecture

### Core Components

- **🔌 Adapters**: Site-specific crawling logic (`src/adapters/`)
- **🗺️ Discovery**: Sitemap parsing and URL discovery (`src/discovery/`)
- **🌐 HTTP**: Conditional GET and caching (`src/http/`)
- **🎯 Selectors**: Configuration-driven extraction rules (`src/selectors/`)
- **📊 Observability**: Metrics and quality monitoring (`src/observability/`)
- **🛡️ Validation**: Integrity gates and provenance checks (`src/validation/`)

### Data Flow

```
Discovery → Product Extraction → Review Extraction → Validation → Storage
    ↓              ↓                    ↓              ↓           ↓
Sitemaps    HTTP + Selectors    Pagination + NLP   Quality Gates  Parquet
```

### Supported Sites

- **Marionnaud.fr**: French beauty retailer
- **Nocibé.fr**: French beauty retailer
- **Extensible**: Add any site via adapter pattern

## Compliance

- Respects robots.txt and Terms of Service
- Implements polite rate limiting (0.5-1 req/sec/domain)
- No PII collection or account logins
- Attribution in User-Agent string
- Comprehensive compliance logging

## Data Outputs

- **Products**: Metadata with refillable detection and luxury classification
- **Reviews**: French-language review corpus with sentiment signals
- **Manifests**: Run metadata and compliance logs
- **Price Statistics**: Category-based luxury thresholds

## License

Research use only. Respect all site terms of service.
