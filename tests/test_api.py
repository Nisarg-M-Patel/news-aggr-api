#!/usr/bin/env python3
"""
Quick test script for all API endpoints
Run this after your API is running on http://localhost:8000
"""

import requests
import json
from datetime import datetime

# Base URL for your API
BASE_URL = "http://localhost:8000/api"

def test_endpoint(method, endpoint, description, expected_status=200, params=None, headers=None):
    """Test a single endpoint and print results"""
    url = f"{BASE_URL}{endpoint}"
    
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"URL: {url}")
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params, headers=headers)
        elif method.upper() == "POST":
            response = requests.post(url, json=params, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == expected_status:
            print("✅ PASS")
            
            # Pretty print JSON response if it's JSON
            try:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    print(f"Response: Found {len(data)} items")
                    print(f"Sample item: {json.dumps(data[0], indent=2, default=str)}")
                elif isinstance(data, dict):
                    print(f"Response: {json.dumps(data, indent=2, default=str)}")
                else:
                    print(f"Response: {data}")
            except:
                print(f"Response: {response.text}")
        else:
            print("❌ FAIL")
            print(f"Expected: {expected_status}, Got: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

def main():
    """Run all endpoint tests"""
    print("🚀 Starting API Endpoint Tests")
    print(f"Base URL: {BASE_URL}")
    
    # Health check
    test_endpoint("GET", "/health", "Health Check")
    
    # Companies endpoints
    test_endpoint("GET", "/companies", "Get All Companies")
    test_endpoint("GET", "/companies?limit=2", "Get Companies with Limit")
    test_endpoint("GET", "/companies?sector=Information Technology", "Get Companies by Sector")
    
    # Sectors endpoints (the one we fixed)
    test_endpoint("GET", "/companies/sectors", "Get All Sectors")
    
    # Try to get a specific company (assuming AAPL exists from seed data)
    test_endpoint("GET", "/companies/AAPL", "Get Specific Company (AAPL)")
    test_endpoint("GET", "/companies/INVALID", "Get Invalid Company", expected_status=404)
    
    # News endpoints
    test_endpoint("GET", "/news", "Get All News")
    test_endpoint("GET", "/news?limit=5", "Get News with Limit")
    test_endpoint("GET", "/news?company_symbol=AAPL", "Get News for AAPL")
    test_endpoint("GET", "/news?days=1", "Get News from Last Day")
    
    # Company-specific news
    test_endpoint("GET", "/companies/AAPL/news", "Get AAPL News")
    test_endpoint("GET", "/companies/AAPL/news?limit=3", "Get AAPL News with Limit")
    test_endpoint("GET", "/companies/INVALID/news", "Get News for Invalid Company", expected_status=404)
    
    # Sector news (if you have companies with this sector)
    test_endpoint("GET", "/sectors/Information Technology/news", "Get IT Sector News")
    test_endpoint("GET", "/sectors/InvalidSector/news", "Get Invalid Sector News", expected_status=404)
    
    # Search endpoint
    test_endpoint("GET", "/search?q=Apple", "Search for 'Apple'")
    test_endpoint("GET", "/search?q=earnings", "Search for 'earnings'")
    test_endpoint("GET", "/search?q=xy", "Search with Short Query", expected_status=422)  # Should fail validation
    
    # Trends endpoint
    test_endpoint("GET", "/trends/sentiment", "Get Sentiment Trends")
    test_endpoint("GET", "/trends/sentiment?days=7", "Get Sentiment Trends (7 days)")
    test_endpoint("GET", "/trends/sentiment?company_symbol=AAPL", "Get AAPL Sentiment Trends")
    
    # Management endpoints
    test_endpoint("POST", "/refresh/AAPL", "Refresh AAPL")
    test_endpoint("POST", "/refresh/all", "Refresh All")
    test_endpoint("POST", "/refresh/INVALID", "Refresh Invalid Company", expected_status=404)
    
    # Debug endpoint
    test_endpoint("GET", "/debug/collect", "Debug Collect News")
    
    print(f"\n{'='*60}")
    print("🏁 Test Complete!")

if __name__ == "__main__":
    main()