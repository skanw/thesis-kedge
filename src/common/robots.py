"""Robots.txt compliance module for polite web crawling."""

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
import httpx
import structlog
from .schema import ComplianceManifest

logger = structlog.get_logger(__name__)


class RobotsParser:
    """Parser for robots.txt files with compliance tracking."""
    
    def __init__(self, robots_dir: Path):
        self.robots_dir = robots_dir
        self.robots_dir.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Tuple[float, Dict]] = {}
        self._cache_ttl = 3600  # 1 hour cache
        
    def fetch_robots(self, domain: str) -> Optional[Dict]:
        """Fetch and parse robots.txt for a domain."""
        robots_url = f"https://{domain}/robots.txt"
        
        # Check cache first
        if domain in self._cache:
            timestamp, rules = self._cache[domain]
            if time.time() - timestamp < self._cache_ttl:
                logger.debug("Using cached robots.txt", domain=domain)
                return rules
        
        try:
            logger.info("Fetching robots.txt", domain=domain, url=robots_url)
            
            with httpx.Client(timeout=10.0) as client:
                response = client.get(robots_url)
                response.raise_for_status()
                
                rules = self._parse_robots_content(response.text, domain)
                
                # Save to file for audit
                robots_file = self.robots_dir / f"{domain}.txt"
                robots_file.write_text(response.text)
                
                # Cache the rules
                self._cache[domain] = (time.time(), rules)
                
                logger.info("Successfully fetched robots.txt", 
                           domain=domain, 
                           allow_paths=len(rules.get("allow", [])),
                           disallow_paths=len(rules.get("disallow", [])))
                
                return rules
                
        except Exception as e:
            logger.warning("Failed to fetch robots.txt", domain=domain, error=str(e))
            # Return permissive default rules
            return {
                "allow": ["/"],
                "disallow": [],
                "crawl_delay": None,
                "user_agent": "*",
                "fetched_at": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def _parse_robots_content(self, content: str, domain: str) -> Dict:
        """Parse robots.txt content into structured rules."""
        rules = {
            "allow": [],
            "disallow": [],
            "crawl_delay": None,
            "user_agent": "*",
            "fetched_at": datetime.now().isoformat()
        }
        
        current_user_agent = "*"
        
        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            if ':' not in line:
                continue
                
            directive, value = line.split(':', 1)
            directive = directive.strip().lower()
            value = value.strip()
            
            if directive == 'user-agent':
                current_user_agent = value
            elif directive == 'allow' and current_user_agent in ['*', 'bot']:
                rules["allow"].append(value)
            elif directive == 'disallow' and current_user_agent in ['*', 'bot']:
                rules["disallow"].append(value)
            elif directive == 'crawl-delay' and current_user_agent in ['*', 'bot']:
                try:
                    rules["crawl_delay"] = float(value)
                except ValueError:
                    logger.warning("Invalid crawl-delay value", domain=domain, value=value)
        
        return rules
    
    def is_allowed(self, url: str, robots_rules: Optional[Dict] = None) -> bool:
        """Check if a URL is allowed by robots.txt rules."""
        if not robots_rules:
            return True
            
        parsed_url = urlparse(url)
        path = parsed_url.path
        
        # Check disallow rules first (more restrictive)
        for disallow_path in robots_rules.get("disallow", []):
            if self._path_matches(path, disallow_path):
                logger.debug("URL disallowed by robots.txt", url=url, disallow_path=disallow_path)
                return False
        
        # Check allow rules
        allow_rules = robots_rules.get("allow", [])
        if allow_rules:
            # If there are explicit allow rules, URL must match at least one
            for allow_path in allow_rules:
                if self._path_matches(path, allow_path):
                    return True
            # If no allow rule matches, deny
            logger.debug("URL not explicitly allowed by robots.txt", url=url)
            return False
        
        # Default allow if no explicit rules
        return True
    
    def _path_matches(self, path: str, rule_path: str) -> bool:
        """Check if a path matches a robots.txt rule pattern."""
        if not rule_path:
            return False
            
        # Handle wildcards
        if '*' in rule_path:
            pattern = rule_path.replace('*', '.*')
            return bool(re.match(pattern, path))
        
        # Exact match or prefix match
        return path == rule_path or path.startswith(rule_path)
    
    def get_crawl_delay(self, robots_rules: Optional[Dict]) -> float:
        """Get crawl delay from robots.txt rules."""
        if not robots_rules:
            return 1.0
            
        delay = robots_rules.get("crawl_delay")
        if delay is not None:
            return float(delay)
        
        return 1.0  # Default polite delay
    
    def create_compliance_manifest(self, domain: str, robots_rules: Optional[Dict]) -> ComplianceManifest:
        """Create compliance manifest for a domain."""
        return ComplianceManifest(
            domain=domain,
            robots_etag=None,  # Could be extracted from response headers
            robots_last_modified=None,  # Could be extracted from response headers
            allow_paths=robots_rules.get("allow", []) if robots_rules else [],
            disallow_paths=robots_rules.get("disallow", []) if robots_rules else [],
            crawl_delay=self.get_crawl_delay(robots_rules),
            start_ts=datetime.now(),
            end_ts=None,
            total_requests=0,
            blocked_requests=0,
            rate_limit_violations=0
        )


class RobotsCompliance:
    """High-level robots.txt compliance manager."""
    
    def __init__(self, robots_dir: Path):
        self.parser = RobotsParser(robots_dir)
        self.manifests: Dict[str, ComplianceManifest] = {}
        
    def check_domain(self, domain: str) -> Tuple[bool, ComplianceManifest]:
        """Check if a domain allows crawling and create compliance manifest."""
        robots_rules = self.parser.fetch_robots(domain)
        manifest = self.parser.create_compliance_manifest(domain, robots_rules)
        
        # Check if product/review paths are allowed
        test_urls = [
            f"https://{domain}/product/test",
            f"https://{domain}/reviews/test",
            f"https://{domain}/p/test"
        ]
        
        allowed_paths = 0
        for test_url in test_urls:
            if self.parser.is_allowed(test_url, robots_rules):
                allowed_paths += 1
        
        is_allowed = allowed_paths > 0
        manifest.allow_paths = robots_rules.get("allow", []) if robots_rules else []
        manifest.disallow_paths = robots_rules.get("disallow", []) if robots_rules else []
        
        self.manifests[domain] = manifest
        
        logger.info("Domain compliance check", 
                   domain=domain, 
                   is_allowed=is_allowed,
                   crawl_delay=manifest.crawl_delay,
                   allow_paths=len(manifest.allow_paths),
                   disallow_paths=len(manifest.disallow_paths))
        
        return is_allowed, manifest
    
    def check_url(self, url: str, domain: str) -> bool:
        """Check if a specific URL is allowed."""
        if domain not in self.manifests:
            _, _ = self.check_domain(domain)
        
        robots_rules = self.parser._cache.get(domain, (0, {}))[1]
        return self.parser.is_allowed(url, robots_rules)
    
    def get_crawl_delay(self, domain: str) -> float:
        """Get crawl delay for a domain."""
        if domain not in self.manifests:
            _, _ = self.check_domain(domain)
        
        return self.manifests[domain].crawl_delay or 1.0
    
    def update_manifest(self, domain: str, **kwargs):
        """Update compliance manifest with new data."""
        if domain in self.manifests:
            for key, value in kwargs.items():
                if hasattr(self.manifests[domain], key):
                    setattr(self.manifests[domain], key, value)
    
    def get_all_manifests(self) -> List[ComplianceManifest]:
        """Get all compliance manifests."""
        return list(self.manifests.values())
