"""CLI interface for the luxury beauty scraping pipeline."""

import asyncio
import sys
from pathlib import Path
from typing import Optional
import click
import structlog
# from .pipeline.ingest import ScrapingPipeline  # Commented out for now

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


@click.group()
@click.option('--config', '-c', default='config.yaml', 
              help='Configuration file path')
@click.option('--brands-file', '-b', default='data/reference/brands_tiers.json',
              help='Brand tiers file path')
@click.option('--output-dir', '-o', default='data',
              help='Output directory')
@click.pass_context
def cli(ctx, config: str, brands_file: str, output_dir: str):
    """Luxury Beauty Scraping Pipeline CLI."""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = Path(config)
    ctx.obj['brands_file'] = Path(brands_file)
    ctx.obj['output_dir'] = Path(output_dir)
    
    # Validate paths
    if not ctx.obj['config_path'].exists():
        click.echo(f"Error: Configuration file not found: {config}")
        sys.exit(1)
    
    if not ctx.obj['brands_file'].exists():
        click.echo(f"Error: Brands file not found: {brands_file}")
        sys.exit(1)
    
    # Create output directory
    ctx.obj['output_dir'].mkdir(parents=True, exist_ok=True)


# @cli.command()
# @click.option('--site', '-s', default='sephora', 
#               type=click.Choice(['sephora', 'marionnaud', 'nocibe']),
#               help='Target e-commerce site')
# @click.option('--max-pages', '-m', default=50, type=int,
#               help='Maximum pages to crawl per category')
# @click.option('--dry-run', is_flag=True,
#               help='Run in dry-run mode (limited data)')
# @click.option('--allow-fixtures', is_flag=True,
#               help='Allow fixture data (default: False)')
# @click.pass_context
# def crawl(ctx, site: str, max_pages: int, dry_run: bool, allow_fixtures: bool):
#     """Crawl products and reviews from e-commerce sites."""
#     try:
#         click.echo(f"Starting crawl for {site}...")
#         
#         # Initialize pipeline
#         pipeline = ScrapingPipeline(
#             config_path=ctx.obj['config_path'],
#             brands_file=ctx.obj['brands_file'],
#             output_dir=ctx.obj['output_dir']
#         )
#         
#         # Run crawl
#         result = asyncio.run(pipeline.run_crawl(
#             site=site,
#             max_pages=max_pages,
#             dry_run=dry_run
#         ))
#         
#         if 'error' in result:
#             click.echo(f"Error: {result['error']}")
#             sys.exit(1)
#         
#         # Display results
#         click.echo(f"\nCrawl completed successfully!")
#         click.echo(f"Run ID: {result['run_id']}")
#         click.echo(f"Products scraped: {len(result['products'])}")
#         click.echo(f"Reviews scraped: {len(result['reviews'])}")
#         click.echo(f"Errors: {result['errors']}")
#         
#         if result['compliance_manifest']:
#             compliance = result['compliance_manifest']
#             click.echo(f"Compliance: {compliance.domain}")
#             click.echo(f"  - Total requests: {compliance.total_requests}")
#             click.echo(f"  - Blocked requests: {compliance.blocked_requests}")
#             click.echo(f"  - Rate limit violations: {compliance.rate_limit_violations}")
#         
#     except Exception as e:
#         click.echo(f"Error: {str(e)}")
#         logger.error("Crawl command failed", error=str(e))
#         sys.exit(1)


@cli.command()
@click.pass_context
def normalize(ctx):
    """Normalize scraped data from bronze to silver layer."""
    try:
        click.echo("Normalizing data...")
        
        # This would implement the normalization logic
        # For now, we'll just show a placeholder
        click.echo("Normalization not yet implemented")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}")
        logger.error("Normalize command failed", error=str(e))
        sys.exit(1)


@cli.command()
@click.pass_context
def validate(ctx):
    """Validate data quality and enforce provenance gates."""
    try:
        click.echo("Running integrity check...")
        
        # Import and run integrity check
        from .validation.integrity_check import IntegrityChecker
        
        checker = IntegrityChecker()
        report = checker.run_integrity_check()
        
        if report['status'] == 'FAIL':
            click.echo(f"❌ Validation failed: {report['total_violations']} violations")
            for violation in report['violations']:
                click.echo(f"  - {violation}")
            sys.exit(1)
        else:
            click.echo(f"✅ Validation passed: {report['audit_sample_size']} products in audit sample")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}")
        logger.error("Validate command failed", error=str(e))
        sys.exit(1)


@cli.command()
@click.option('--out', '-o', default='data/silver',
              help='Output directory for Parquet files')
@click.pass_context
def export(ctx, out: str):
    """Export data to Parquet format."""
    try:
        click.echo(f"Exporting data to {out}...")
        
        # This would implement the export logic
        # For now, we'll just show a placeholder
        click.echo("Export not yet implemented")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}")
        logger.error("Export command failed", error=str(e))
        sys.exit(1)


@cli.command()
@click.pass_context
def price_backstop(ctx):
    """Compute price statistics and luxury classification."""
    try:
        click.echo("Computing price statistics...")
        
        # This would implement the price backstop logic
        # For now, we'll just show a placeholder
        click.echo("Price backstop not yet implemented")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}")
        logger.error("Price backstop command failed", error=str(e))
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show pipeline status and recent runs."""
    try:
        click.echo("Pipeline Status:")
        click.echo(f"  Config: {ctx.obj['config_path']}")
        click.echo(f"  Brands: {ctx.obj['brands_file']}")
        click.echo(f"  Output: {ctx.obj['output_dir']}")
        
        # Check if output directory has data
        output_dir = ctx.obj['output_dir']
        if output_dir.exists():
            files = list(output_dir.rglob('*.parquet'))
            click.echo(f"  Parquet files: {len(files)}")
            
            manifests = list(output_dir.rglob('*manifest*.json'))
            click.echo(f"  Manifest files: {len(manifests)}")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}")
        logger.error("Status command failed", error=str(e))
        sys.exit(1)


@cli.command()
@click.pass_context
def test(ctx):
    """Run tests to verify pipeline functionality."""
    try:
        click.echo("Running tests...")
        
        # This would run the test suite
        # For now, we'll just show a placeholder
        click.echo("Tests not yet implemented")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}")
        logger.error("Test command failed", error=str(e))
        sys.exit(1)


@cli.command()
@click.option('--site', required=True, help='Website to crawl (e.g., marionnaud.fr)')
@click.option('--max-pages', default=10, help='Maximum pages to crawl')
@click.option('--rate-limit', default=0.5, help='Requests per second')
@click.pass_context
def crawl_real(ctx, site: str, max_pages: int, rate_limit: float):
    """Crawl real websites with proper compliance and robots.txt respect."""
    try:
        from src.crawlers.real_crawler import RealWebCrawler
        
        click.echo(f"🕷️  Starting real crawl of {site}")
        click.echo(f"📄 Max pages: {max_pages}")
        click.echo(f"⏱️  Rate limit: {rate_limit} RPS")
        
        config = {
            'rate_limit_rps': rate_limit,
            'min_rps': 0.1,
            'max_rps': 1.0,
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        async def run_crawl():
            async with RealWebCrawler(config) as crawler:
                results = await crawler.crawl_site(f"https://www.{site}", max_pages)
                
                click.echo(f"✅ Crawl completed!")
                click.echo(f"📦 Products found: {len(results['products'])}")
                click.echo(f"💬 Reviews found: {len(results['reviews'])}")
                click.echo(f"📄 Pages visited: {results['total_pages_visited']}")
                
                # Save results
                if results['products']:
                    import pandas as pd
                    df_products = pd.DataFrame(results['products'])
                    df_products.to_parquet(f'data/silver/products_{site}.parquet', index=False)
                    click.echo(f"💾 Products saved to data/silver/products_{site}.parquet")
                
                if results['reviews']:
                    import pandas as pd
                    df_reviews = pd.DataFrame(results['reviews'])
                    df_reviews.to_parquet(f'data/silver/reviews_{site}.parquet', index=False)
                    click.echo(f"💾 Reviews saved to data/silver/reviews_{site}.parquet")
        
        asyncio.run(run_crawl())
        
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        logger.error("Real crawl command failed", error=str(e))
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {str(e)}")
        logger.error("CLI main failed", error=str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
