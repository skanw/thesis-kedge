"""HTTP utilities for polite web crawling with retry and rate limiting."""

import time
import random
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
import httpx
import structlog
from playwright.async_api import async_playwright, Browser, Page
from .schema import PageManifest

logger = structlog.get_logger(__name__)


class RateLimiter:
    """Rate limiter for polite crawling."""
    
    def __init__(self, requests_per_second: float = 1.0):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time: Dict[str, float] = {}
        
    def wait_if_needed(self, domain: str):
        """Wait if needed to respect rate limits."""
        now = time.time()
        last_time = self.last_request_time.get(domain, 0)
        
        time_since_last = now - last_time
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time[domain] = time.time()


class HTTPClient:
    """HTTP client with retry logic and rate limiting."""
    
    def __init__(self, 
                 user_agents: List[str],
                 rate_limiter: RateLimiter,
                 timeout: float = 30.0,
                 max_retries: int = 3):
        self.user_agents = user_agents
        self.rate_limiter = rate_limiter
        self.timeout = timeout
        self.max_retries = max_retries
        self.session: Optional[httpx.Client] = None
        
    def __enter__(self):
        self.session = httpx.Client(timeout=self.timeout)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent from the list."""
        return random.choice(self.user_agents)
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        return urlparse(url).netloc
    
    def get(self, url: str, **kwargs) -> httpx.Response:
        """Make a GET request with rate limiting and retry logic."""
        domain = self._get_domain(url)
        
        # Rate limiting
        self.rate_limiter.wait_if_needed(domain)
        
        # Add user agent
        headers = kwargs.get('headers', {})
        headers['User-Agent'] = self._get_random_user_agent()
        kwargs['headers'] = headers
        
        # Retry logic
        for attempt in range(self.max_retries + 1):
            try:
                start_time = time.time()
                response = self.session.get(url, **kwargs)
                response_time = (time.time() - start_time) * 1000
                
                logger.debug("HTTP request completed",
                           url=url,
                           status_code=response.status_code,
                           response_time_ms=response_time,
                           attempt=attempt + 1)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait_time = int(retry_after)
                        except ValueError:
                            wait_time = 60
                    else:
                        wait_time = 60 * (2 ** attempt)  # Exponential backoff
                    
                    logger.warning("Rate limited, waiting", 
                                 url=url, 
                                 wait_time=wait_time,
                                 attempt=attempt + 1)
                    time.sleep(wait_time)
                    continue
                
                # Handle server errors
                if response.status_code >= 500:
                    if attempt < self.max_retries:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.warning("Server error, retrying",
                                     url=url,
                                     status_code=response.status_code,
                                     wait_time=wait_time,
                                     attempt=attempt + 1)
                        time.sleep(wait_time)
                        continue
                
                return response
                
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    logger.warning("Connection error, retrying",
                                 url=url,
                                 error=str(e),
                                 wait_time=wait_time,
                                 attempt=attempt + 1)
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        # This should never be reached
        raise httpx.RequestError("Max retries exceeded")
    
    def create_page_manifest(self, url: str, response: httpx.Response, 
                           robots_allowed: bool, crawl_delay: Optional[float] = None) -> PageManifest:
        """Create page manifest for auditability."""
        return PageManifest(
            url=url,
            site=self._get_domain(url),
            scrape_ts=datetime.now(),
            status_code=response.status_code,
            content_length=len(response.content) if response.content else None,
            html_hash=None,  # Could be computed if needed
            robots_allowed=robots_allowed,
            crawl_delay=crawl_delay,
            user_agent=response.request.headers.get('User-Agent', ''),
            response_time_ms=None  # Could be tracked if needed
        )


class PlaywrightClient:
    """Playwright client for JavaScript-rendered content."""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        
    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.page = await self.browser.new_page()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()
    
    async def get_page_content(self, url: str, wait_for_selector: Optional[str] = None) -> str:
        """Get page content with optional wait for selector."""
        try:
            await self.page.goto(url, wait_until='networkidle')
            
            if wait_for_selector:
                await self.page.wait_for_selector(wait_for_selector, timeout=10000)
            
            # Wait a bit more for dynamic content
            await self.page.wait_for_timeout(2000)
            
            content = await self.page.content()
            return content
            
        except Exception as e:
            logger.error("Failed to get page content", url=url, error=str(e))
            raise
    
    async def get_json_from_xhr(self, url: str, xhr_url: str) -> Dict[str, Any]:
        """Get JSON data from XHR endpoint."""
        try:
            # Navigate to the page first
            await self.page.goto(url, wait_until='networkidle')
            
            # Wait for XHR request to complete
            response = await self.page.wait_for_response(xhr_url, timeout=10000)
            
            if response.ok:
                return response.json()
            else:
                logger.warning("XHR request failed", url=xhr_url, status=response.status)
                return {}
                
        except Exception as e:
            logger.error("Failed to get XHR data", url=url, xhr_url=xhr_url, error=str(e))
            return {}


class SessionManager:
    """Session manager for coordinating HTTP and Playwright clients."""
    
    def __init__(self, 
                 user_agents: List[str],
                 rate_limiter: RateLimiter,
                 timeout: float = 30.0,
                 max_retries: int = 3):
        self.user_agents = user_agents
        self.rate_limiter = rate_limiter
        self.timeout = timeout
        self.max_retries = max_retries
        self.http_client: Optional[HTTPClient] = None
        self.playwright_client: Optional[PlaywrightClient] = None
        
    def get_http_client(self) -> HTTPClient:
        """Get HTTP client instance."""
        if not self.http_client:
            self.http_client = HTTPClient(
                user_agents=self.user_agents,
                rate_limiter=self.rate_limiter,
                timeout=self.timeout,
                max_retries=self.max_retries
            )
        return self.http_client
    
    async def get_playwright_client(self, headless: bool = True) -> PlaywrightClient:
        """Get Playwright client instance."""
        if not self.playwright_client:
            self.playwright_client = PlaywrightClient(headless=headless)
            await self.playwright_client.__aenter__()
        return self.playwright_client
    
    async def close(self):
        """Close all clients."""
        if self.playwright_client:
            await self.playwright_client.__aexit__(None, None, None)
            self.playwright_client = None
        
        if self.http_client:
            self.http_client.__exit__(None, None, None)
            self.http_client = None
