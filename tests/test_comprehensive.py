#!/usr/bin/env python3
"""
Comprehensive test suite for the news API
Run with: pytest tests/test_comprehensive.py -v
"""

import pytest
import requests
import time
from datetime import datetime, timedelta
import json

# Test configuration
BASE_URL = "http://localhost:8000/api"
TIMEOUT = 10

class TestAPI:
    """Test the main API endpoints"""
    
    def test_health_check(self):
        """Test basic health endpoint"""
        response = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_get_companies(self):
        """Test companies endpoint"""
        response = requests.get(f"{BASE_URL}/companies", timeout=TIMEOUT)
        assert response.status_code == 200
        
        companies = response.json()
        assert isinstance(companies, list)
        assert len(companies) > 0
        
        # Check company structure
        company = companies[0]
        assert "id" in company
        assert "symbol" in company
        assert "name" in company
        assert "sector" in company
    
    def test_get_sectors(self):
        """Test sectors endpoint"""
        response = requests.get(f"{BASE_URL}/companies/sectors", timeout=TIMEOUT)
        assert response.status_code == 200
        
        sectors = response.json()
        assert isinstance(sectors, list)
        assert len(sectors) > 0
        
        # Check sector structure
        sector = sectors[0]
        assert "sector" in sector
        assert "company_count" in sector
    
    def test_get_specific_company(self):
        """Test getting a specific company"""
        response = requests.get(f"{BASE_URL}/companies/AAPL", timeout=TIMEOUT)
        assert response.status_code == 200
        
        company = response.json()
        assert company["symbol"] == "AAPL"
        assert "Apple" in company["name"]
    
    def test_get_nonexistent_company(self):
        """Test getting a company that doesn't exist"""
        response = requests.get(f"{BASE_URL}/companies/FAKE", timeout=TIMEOUT)
        assert response.status_code == 404

class TestNewsEndpoints:
    """Test news-related endpoints"""
    
    def test_company_news_basic(self):
        """Test basic company news endpoint"""
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL"}, timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert "page" in data
        assert "size" in data
        assert "pages" in data
        
        # Check pagination structure
        assert isinstance(data["items"], list)
        assert isinstance(data["total"], int)
        assert data["page"] >= 1
    
    def test_company_news_with_days(self):
        """Test company news with days parameter"""
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL", "days": 7}, timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert "items" in data
    
    def test_company_news_pagination(self):
        """Test company news pagination"""
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL", "days": 30, "limit": 5}, timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert data["size"] == 5
        assert len(data["items"]) <= 5
    
    def test_company_news_invalid_company(self):
        """Test company news with invalid company"""
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "FAKE"}, timeout=TIMEOUT)
        assert response.status_code == 404
    
    def test_sector_news(self):
        """Test sector news endpoint"""
        response = requests.get(f"{BASE_URL}/news/sector", params={"s": "Information Technology"}, timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert "items" in data
        assert "total" in data
    
    def test_sector_news_invalid_sector(self):
        """Test sector news with invalid sector"""
        response = requests.get(f"{BASE_URL}/news/sector", params={"s": "Fake Sector"}, timeout=TIMEOUT)
        assert response.status_code == 404

class TestNewsArticleStructure:
    """Test the structure of news articles returned by API"""
    
    def test_news_article_structure(self):
        """Test that news articles have the correct structure"""
        # Get some news
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL", "days": 30}, timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        if data["items"]:
            article = data["items"][0]
            
            # Required fields
            assert "id" in article
            assert "title" in article
            assert "url" in article
            assert "source" in article
            assert "published_at" in article
            assert "fetched_at" in article
            assert "category" in article
            assert "companies" in article
            
            # Check types
            assert isinstance(article["id"], int)
            assert isinstance(article["title"], str)
            assert isinstance(article["url"], str)
            assert isinstance(article["source"], str)
            assert isinstance(article["companies"], list)
            
            # URL should be valid format
            assert article["url"].startswith("http")
            
            # Title should not be empty
            assert len(article["title"]) > 0

class TestDebugEndpoints:
    """Test debug and management endpoints"""
    
    def test_debug_collect(self):
        """Test debug collection endpoint"""
        response = requests.get(f"{BASE_URL}/debug/collect", timeout=30)  # Longer timeout for collection
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert data["status"] in ["success", "info", "warning", "error"]
    
    def test_refresh_company(self):
        """Test company refresh endpoint"""
        response = requests.post(f"{BASE_URL}/refresh/AAPL", timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "success"
    
    def test_refresh_invalid_company(self):
        """Test refresh for invalid company"""
        response = requests.post(f"{BASE_URL}/refresh/FAKE", timeout=TIMEOUT)
        assert response.status_code == 404
    
    def test_refresh_all(self):
        """Test refresh all endpoint"""
        response = requests.post(f"{BASE_URL}/refresh/all", timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "success"

class TestDataIntegrity:
    """Test data integrity and consistency"""
    
    def test_companies_have_news_or_not(self):
        """Test that companies either have news or don't, consistently"""
        # Get all companies
        companies_response = requests.get(f"{BASE_URL}/companies", timeout=TIMEOUT)
        assert companies_response.status_code == 200
        companies = companies_response.json()
        
        # Check a few companies for news
        for company in companies[:3]:  # Test first 3
            symbol = company["symbol"]
            news_response = requests.get(f"{BASE_URL}/news/company", params={"c": symbol, "days": 30}, timeout=TIMEOUT)
            assert news_response.status_code == 200
            
            news_data = news_response.json()
            # Should have valid pagination even if no news
            assert "total" in news_data
            assert news_data["total"] >= 0
    
    def test_news_articles_reference_valid_companies(self):
        """Test that news articles only reference companies that exist"""
        # Get some news
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL", "days": 30}, timeout=TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        if data["items"]:
            article = data["items"][0]
            
            # Check that referenced companies exist
            for company in article["companies"]:
                assert "id" in company
                assert "symbol" in company
                assert "name" in company
                
                # Verify company exists in system
                company_response = requests.get(f"{BASE_URL}/companies/{company['symbol']}", timeout=TIMEOUT)
                assert company_response.status_code == 200

class TestPerformance:
    """Basic performance tests"""
    
    def test_response_times(self):
        """Test that API responses are reasonably fast"""
        endpoints = [
            "/health",
            "/companies",
            "/companies/sectors",
            "/companies/AAPL",
        ]
        
        for endpoint in endpoints:
            start_time = time.time()
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=TIMEOUT)
            end_time = time.time()
            
            assert response.status_code == 200
            response_time = (end_time - start_time) * 1000  # Convert to ms
            assert response_time < 2000, f"Endpoint {endpoint} took {response_time:.1f}ms (too slow)"
    
    def test_concurrent_requests(self):
        """Test that API can handle multiple concurrent requests"""
        import concurrent.futures
        import threading
        
        def make_request():
            response = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
            return response.status_code == 200
        
        # Make 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # All requests should succeed
        assert all(results), "Some concurrent requests failed"

class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_invalid_parameters(self):
        """Test API behavior with invalid parameters"""
        # Invalid days parameter
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL", "days": -1}, timeout=TIMEOUT)
        # Should either work or return 422, but not crash
        assert response.status_code in [200, 422]
        
        # Invalid limit parameter
        response = requests.get(f"{BASE_URL}/news/company", params={"c": "AAPL", "limit": 0}, timeout=TIMEOUT)
        assert response.status_code in [200, 422]
    
    def test_missing_required_parameters(self):
        """Test endpoints with missing required parameters"""
        # Company news without company parameter
        response = requests.get(f"{BASE_URL}/news/company", timeout=TIMEOUT)
        assert response.status_code == 422  # Validation error
        
        # Sector news without sector parameter
        response = requests.get(f"{BASE_URL}/news/sector", timeout=TIMEOUT)
        assert response.status_code == 422  # Validation error

# Utility functions for running tests
def run_quick_tests():
    """Run just the essential tests for quick validation"""
    print("🚀 Running Quick Test Suite...")
    
    quick_tests = [
        TestAPI().test_health_check,
        TestAPI().test_get_companies,
        TestNewsEndpoints().test_company_news_basic,
        TestDebugEndpoints().test_debug_collect,
    ]
    
    passed = 0
    failed = 0
    
    for test in quick_tests:
        try:
            test()
            print(f"✅ {test.__name__}")
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__}: {str(e)}")
            failed += 1
    
    print(f"\n📊 Quick Test Results: {passed} passed, {failed} failed")
    return failed == 0

if __name__ == "__main__":
    """Run tests directly or with pytest"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # Run quick tests
        success = run_quick_tests()
        sys.exit(0 if success else 1)
    else:
        print("💡 Usage:")
        print("  python test_comprehensive.py --quick     # Run quick tests")
        print("  pytest test_comprehensive.py -v         # Run all tests with pytest")
        print("  pytest test_comprehensive.py::TestAPI -v # Run specific test class")