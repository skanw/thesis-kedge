"""Tests for robots.txt compliance module."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from src.common.robots import RobotsParser, RobotsCompliance
from src.common.schema import ComplianceManifest


class TestRobotsParser:
    """Test robots.txt parser functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.robots_dir = Path("/tmp/test_robots")
        self.robots_dir.mkdir(exist_ok=True)
        self.parser = RobotsParser(self.robots_dir)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        if self.robots_dir.exists():
            shutil.rmtree(self.robots_dir)
    
    def test_parse_robots_content(self):
        """Test parsing robots.txt content."""
        content = """
User-agent: *
Allow: /product/
Allow: /reviews/
Disallow: /admin/
Disallow: /private/
Crawl-delay: 2
"""
        
        rules = self.parser._parse_robots_content(content, "test.com")
        
        assert rules["allow"] == ["/product/", "/reviews/"]
        assert rules["disallow"] == ["/admin/", "/private/"]
        assert rules["crawl_delay"] == 2.0
        assert rules["user_agent"] == "*"
    
    def test_path_matches_exact(self):
        """Test exact path matching."""
        assert self.parser._path_matches("/product/123", "/product/")
        assert self.parser._path_matches("/product/123", "/product/123")
        assert not self.parser._path_matches("/product/123", "/admin/")
    
    def test_path_matches_wildcard(self):
        """Test wildcard path matching."""
        assert self.parser._path_matches("/product/123", "/product/*")
        assert self.parser._path_matches("/reviews/456", "/*")
        assert not self.parser._path_matches("/admin/", "/product/*")
    
    def test_is_allowed(self):
        """Test URL allowance checking."""
        rules = {
            "allow": ["/product/"],
            "disallow": ["/admin/"],
            "crawl_delay": 1.0
        }
        
        assert self.parser.is_allowed("https://test.com/product/123", rules)
        assert not self.parser.is_allowed("https://test.com/admin/", rules)
        assert self.parser.is_allowed("https://test.com/other/", rules)  # Default allow
    
    def test_get_crawl_delay(self):
        """Test crawl delay extraction."""
        rules = {"crawl_delay": 2.5}
        assert self.parser.get_crawl_delay(rules) == 2.5
        
        rules = {}
        assert self.parser.get_crawl_delay(rules) == 1.0  # Default


class TestRobotsCompliance:
    """Test robots compliance manager."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.robots_dir = Path("/tmp/test_robots_compliance")
        self.robots_dir.mkdir(exist_ok=True)
        self.compliance = RobotsCompliance(self.robots_dir)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        if self.robots_dir.exists():
            shutil.rmtree(self.robots_dir)
    
    @patch('src.common.robots.httpx.Client')
    def test_check_domain_success(self, mock_client):
        """Test successful domain compliance check."""
        # Mock response
        mock_response = Mock()
        mock_response.text = """
User-agent: *
Allow: /product/
Allow: /reviews/
Crawl-delay: 1
"""
        mock_response.raise_for_status.return_value = None
        
        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client.return_value.__enter__.return_value = mock_client_instance
        
        is_allowed, manifest = self.compliance.check_domain("test.com")
        
        assert is_allowed
        assert isinstance(manifest, ComplianceManifest)
        assert manifest.domain == "test.com"
        assert manifest.crawl_delay == 1.0
        assert "/product/" in manifest.allow_paths
        assert "/reviews/" in manifest.allow_paths
    
    @patch('src.common.robots.httpx.Client')
    def test_check_domain_failure(self, mock_client):
        """Test domain compliance check with failure."""
        # Mock failure
        mock_client_instance = Mock()
        mock_client_instance.get.side_effect = Exception("Connection failed")
        mock_client.return_value.__enter__.return_value = mock_client_instance
        
        is_allowed, manifest = self.compliance.check_domain("test.com")
        
        # Should return permissive defaults on failure
        assert is_allowed
        assert isinstance(manifest, ComplianceManifest)
        assert manifest.domain == "test.com"
    
    def test_check_url(self):
        """Test URL compliance checking."""
        # Set up mock robots rules
        self.compliance.parser._cache["test.com"] = (0, {
            "allow": ["/product/"],
            "disallow": ["/admin/"],
            "crawl_delay": 1.0
        })
        
        assert self.compliance.check_url("https://test.com/product/123", "test.com")
        assert not self.compliance.check_url("https://test.com/admin/", "test.com")
    
    def test_get_crawl_delay(self):
        """Test crawl delay retrieval."""
        # Set up mock manifest
        manifest = ComplianceManifest(
            domain="test.com",
            crawl_delay=2.5,
            start_ts=None,
            allow_paths=[],
            disallow_paths=[]
        )
        self.compliance.manifests["test.com"] = manifest
        
        assert self.compliance.get_crawl_delay("test.com") == 2.5
    
    def test_update_manifest(self):
        """Test manifest updating."""
        manifest = ComplianceManifest(
            domain="test.com",
            start_ts=None,
            allow_paths=[],
            disallow_paths=[]
        )
        self.compliance.manifests["test.com"] = manifest
        
        self.compliance.update_manifest("test.com", total_requests=100)
        
        assert self.compliance.manifests["test.com"].total_requests == 100
    
    def test_get_all_manifests(self):
        """Test getting all manifests."""
        manifest1 = ComplianceManifest(
            domain="test1.com",
            start_ts=None,
            allow_paths=[],
            disallow_paths=[]
        )
        manifest2 = ComplianceManifest(
            domain="test2.com",
            start_ts=None,
            allow_paths=[],
            disallow_paths=[]
        )
        
        self.compliance.manifests["test1.com"] = manifest1
        self.compliance.manifests["test2.com"] = manifest2
        
        all_manifests = self.compliance.get_all_manifests()
        assert len(all_manifests) == 2
        assert all(isinstance(m, ComplianceManifest) for m in all_manifests)
