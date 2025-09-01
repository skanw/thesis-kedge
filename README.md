# Luxury Beauty Scraping Pipeline

A legally compliant, reproducible, and scalable scraping pipeline for French luxury beauty e-commerce reviews and product metadata.

## Research Topic
Big-Data Customer Segmentation in French Luxury Beauty: clustering retail review behavior and "Refillable" sustainability signals.

## Features

- **Legally Compliant**: Respects robots.txt, implements polite crawling, no PII collection
- **Research-Ready Data**: Extracts product metadata, review corpus, and sustainability signals
- **Luxury Filtering**: Brand-first inclusion with price backstop validation
- **Refillable Detection**: Multi-source evidence for sustainability analysis
- **Incremental Crawling**: Supports incremental runs with deduplication
- **Data Quality**: Great Expectations validation and comprehensive testing

## Quick Start

```bash
# Install dependencies
poetry install

# Run a dry-run crawl
poetry run lux-beauty crawl --site sephora --dry-run

# Normalize and validate data
poetry run lux-beauty normalize
poetry run lux-beauty validate

# Export to Parquet
poetry run lux-beauty export --out data/silver
```

## Architecture

- **Common**: Shared utilities for HTTP, parsing, validation, and compliance
- **Site Adapters**: Modular scrapers for Sephora, Marionnaud, Nocibé
- **Pipeline**: Orchestration, normalization, deduplication, and export
- **Data**: Bronze (raw), Silver (normalized), and reference data

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
