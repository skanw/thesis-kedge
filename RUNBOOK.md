# Luxury Beauty Scraping Pipeline - Runbook

## Overview

This runbook provides step-by-step instructions for operating the luxury beauty scraping pipeline for French e-commerce sites. The pipeline extracts product metadata, reviews, and sustainability signals for research purposes.

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd lux_beauty

# Install dependencies
poetry install

# Install Playwright browsers
poetry run playwright install chromium
```

### 2. First Run

```bash
# Run a dry-run crawl
poetry run lux-beauty crawl --site sephora --dry-run

# Check status
poetry run lux-beauty status
```

## Detailed Operations

### 1. Update Brand Tiers

The brand tiers file (`data/reference/brands_tiers.json`) contains luxury brand classifications:

```json
{
  "tiers": {
    "1": ["Chanel", "Parfums Christian Dior", "Guerlain", ...],
    "1.5": ["Valentino Beauty", "Burberry Beauty", ...]
  }
}
```

**To update brand tiers:**

1. Edit `data/reference/brands_tiers.json`
2. Add new brands to appropriate tiers (1 = ultra-luxury, 1.5 = premium)
3. Use exact brand names as they appear on e-commerce sites
4. Restart the pipeline to use updated tiers

**Example:**
```bash
# Add a new luxury brand
echo '    "Maison Margiela",' >> data/reference/brands_tiers.json

# Run crawl with updated tiers
poetry run lux-beauty crawl --site sephora --max-pages 10
```

### 2. Add a New Retailer Adapter

To add support for a new e-commerce site:

1. **Create site directory:**
   ```bash
   mkdir -p src/new_retailer
   ```

2. **Create selectors file:**
   ```python
   # src/new_retailer/selectors.py
   PRODUCT_SELECTORS = {
       "product_id": ["[data-product-id]", ".product-id"],
       "brand": [".product-brand", "[data-brand]"],
       # ... other selectors
   }
   ```

3. **Create scrapers:**
   ```python
   # src/new_retailer/product_scraper.py
   class NewRetailerProductScraper:
       def scrape_product(self, html: str, url: str) -> Optional[Product]:
           # Implementation
           pass
   ```

4. **Update configuration:**
   ```yaml
   # config.yaml
   sites:
     new_retailer:
       domain: "newretailer.fr"
       base_url: "https://www.newretailer.fr"
       category_urls:
         - "https://www.newretailer.fr/parfums"
   ```

5. **Update pipeline:**
   ```python
   # src/pipeline/ingest.py
   # Add import and initialization for new retailer
   ```

### 3. Run a 24-Hour Polite Crawl

For extended data collection:

```bash
# Start a comprehensive crawl
poetry run lux-beauty crawl \
  --site sephora \
  --max-pages 200 \
  --config config.yaml \
  --brands-file data/reference/brands_tiers.json

# Monitor progress
poetry run lux-beauty status

# Check logs
tail -f logs/scraping.log
```

**Configuration for 24-hour crawl:**

```yaml
# config.yaml
crawling:
  rate_limit_rps: 0.5  # Slower rate for extended crawl
  default_delay_seconds: 2.0
  max_retries: 5
  timeout_seconds: 60

# Use multiple user agents
user_agents:
  - "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 [Research: French Luxury Beauty Analysis]"
  - "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 [Research: French Luxury Beauty Analysis]"
```

**Monitoring during crawl:**

```bash
# Check disk space
df -h data/

# Monitor memory usage
htop

# Check compliance logs
ls -la data/reference/robots/

# View recent manifests
ls -la data/*manifest*.json
```

### 4. Generate Kept/Dropped Luxury Report

After crawling, generate the luxury classification report:

```bash
# Run price backstop analysis
poetry run lux-beauty price_backstop

# View the report
cat data/silver/price_stats.parquet
```

**Understanding the report:**

- **Kept products:** `brand_in_tier AND price >= p75(category)`
- **Dropped products:** Below price threshold or non-luxury brand
- **Price percentiles:** p25, p50, p75, p90 per category

**Example report interpretation:**
```
Category: fragrance_edp_edt
- p75 threshold: €120.00
- Kept products: 1,234 (luxury brands above threshold)
- Dropped products: 567 (below threshold or non-luxury)
- Refillable products: 89 (7.2% of kept products)
```

### 5. Compute Initial Descriptive Stats

Generate comprehensive statistics:

```bash
# Run validation to get quality metrics
poetry run lux-beauty validate

# Export to Parquet for analysis
poetry run lux-beauty export --out data/silver

# Analyze with Python
python -c "
import polars as pl

# Load data
products = pl.read_parquet('data/silver/products.parquet')
reviews = pl.read_parquet('data/silver/reviews.parquet')

# Brand counts
brand_stats = products.group_by('brand').agg([
    pl.count().alias('product_count'),
    pl.mean('price_value').alias('avg_price'),
    pl.sum('refillable_flag').alias('refillable_count')
]).sort('product_count', descending=True)

print('Top brands by product count:')
print(brand_stats.head(10))

# Refillable analysis
refillable_stats = products.filter(pl.col('refillable_flag')).group_by('brand').agg([
    pl.count().alias('refillable_products'),
    pl.mean('price_value').alias('avg_refillable_price')
]).sort('refillable_products', descending=True)

print('\\nRefillable products by brand:')
print(refillable_stats.head(10))

# Price distributions
price_stats = products.group_by('category_path').agg([
    pl.count().alias('count'),
    pl.mean('price_value').alias('mean_price'),
    pl.std('price_value').alias('std_price'),
    pl.quantile('price_value', 0.75).alias('p75_price')
])

print('\\nPrice statistics by category:')
print(price_stats)
"
```

## Troubleshooting

### Common Issues

1. **Rate limiting (429 errors):**
   ```bash
   # Reduce rate limit
   # Edit config.yaml
   crawling:
     rate_limit_rps: 0.3  # Reduce from 1.0
     default_delay_seconds: 3.0  # Increase delay
   ```

2. **Robots.txt blocking:**
   ```bash
   # Check robots.txt compliance
   cat data/reference/robots/sephora.fr.txt
   
   # Disable strict compliance (not recommended)
   # Edit config.yaml
   crawling:
     strict_compliance: false
   ```

3. **Memory issues:**
   ```bash
   # Monitor memory usage
   htop
   
   # Reduce concurrency
   # Edit config.yaml
   crawling:
     concurrency_per_site: 1  # Reduce from 2
   ```

4. **Disk space:**
   ```bash
   # Check disk usage
   du -sh data/
   
   # Clean old files
   find data/raw -name "*.html" -mtime +7 -delete
   ```

### Log Analysis

```bash
# View recent logs
tail -f logs/scraping.log

# Search for errors
grep "ERROR" logs/scraping.log

# Check compliance violations
grep "rate_limit_violations" logs/scraping.log

# Monitor progress
grep "Products scraped" logs/scraping.log
```

## Data Quality Checks

### Validation Commands

```bash
# Run full validation
poetry run lux-beauty validate

# Check data completeness
python -c "
import polars as pl
products = pl.read_parquet('data/silver/products.parquet')

# Required fields
required_fields = ['product_id', 'brand', 'name', 'price_value']
missing_data = products.select([
    pl.col(field).is_null().alias(f'missing_{field}')
    for field in required_fields
]).sum()

print('Missing data summary:')
print(missing_data)
"
```

### Quality Metrics

- **Products with ratings:** ≥ 90%
- **Products with required fields:** ≥ 95%
- **French reviews ratio:** ≥ 80%
- **Refillable evidence:** All refillable products must have evidence

## Compliance Monitoring

### Robots.txt Compliance

```bash
# Check robots.txt snapshots
ls -la data/reference/robots/

# View compliance manifests
cat data/*compliance*.json

# Monitor compliance during crawl
grep "robots_allowed" logs/scraping.log
```

### Rate Limiting

```bash
# Check rate limit violations
grep "rate_limit_violations" logs/scraping.log

# Monitor request timing
grep "response_time_ms" logs/scraping.log
```

## Performance Optimization

### For Large Crawls

1. **Increase concurrency:**
   ```yaml
   crawling:
     concurrency_per_site: 4  # Increase from 2
   ```

2. **Use multiple sites:**
   ```bash
   # Run parallel crawls
   poetry run lux-beauty crawl --site sephora &
   poetry run lux-beauty crawl --site marionnaud &
   wait
   ```

3. **Optimize storage:**
   ```bash
   # Use Parquet compression
   # Edit export settings in config.yaml
   storage:
     compression: "snappy"
   ```

## Backup and Recovery

### Data Backup

```bash
# Create backup
tar -czf backup_$(date +%Y%m%d).tar.gz data/

# Restore from backup
tar -xzf backup_20240101.tar.gz
```

### Incremental Crawls

```bash
# Resume from last run
poetry run lux-beauty crawl --site sephora --resume

# Check last run ID
cat data/*run_manifest*.json | jq '.run_id'
```

## Security and Privacy

### Data Protection

- No PII collection (only anonymized labels)
- Research attribution in User-Agent
- Respect robots.txt and Terms of Service
- Rate limiting to avoid server impact

### Access Control

```bash
# Set proper permissions
chmod 600 config.yaml
chmod 700 data/

# Use non-root user in Docker
docker run --user 1000:1000 lux-beauty
```

## Support and Maintenance

### Regular Maintenance

1. **Weekly:**
   - Check disk space
   - Review compliance logs
   - Update brand tiers if needed

2. **Monthly:**
   - Update selectors for site changes
   - Review rate limiting effectiveness
   - Clean old data files

3. **Quarterly:**
   - Full validation run
   - Performance review
   - Compliance audit

### Getting Help

- Check logs: `logs/scraping.log`
- Review manifests: `data/*manifest*.json`
- Validate data: `poetry run lux-beauty validate`
- Test functionality: `poetry run lux-beauty test`
