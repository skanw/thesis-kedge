#!/usr/bin/env python3
"""Data validation pipeline."""

import argparse
import sys
from pathlib import Path
import structlog

logger = structlog.get_logger()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Luxury Beauty Data Validation Pipeline")
    args = parser.parse_args()
    
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
    
    try:
        # Import and run integrity check
        from ..validation.integrity_check import IntegrityChecker
        
        checker = IntegrityChecker()
        report = checker.run_integrity_check()
        
        if report['status'] == 'FAIL':
            logger.error(f"❌ Validation failed: {report['total_violations']} violations")
            for violation in report['violations']:
                logger.error(f"  - {violation}")
            sys.exit(1)
        else:
            logger.info(f"✅ Validation passed: {report['audit_sample_size']} products in audit sample")
            print(f"✅ Validation passed: {report['audit_sample_size']} products in audit sample")
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
