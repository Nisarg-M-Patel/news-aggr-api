#!/usr/bin/env python3
"""
Simple test script for the clean news API
Run this after your API is running on http://localhost:8000
"""

import requests
import json

# Base URL for your API
BASE_URL = "http://localhost:8000/api"

def test_endpoint(method, endpoint, description, expected_status=200, params=None):
    """Test a single endpoint and print results"""
    url = f"{BASE_URL}{endpoint}"
    
    print(f"\n{'='*50}")
    print(f"Testing: {description}")
    print(f"URL: {url}")
    if params:
        print(f"Params: {params}")
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, json=params)
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == expected_status:
            print("✅ PASS")
            
            try:
                data = response.json()
                if isinstance(data, dict) and 'items' in data:
                    print(f"Found {data['total']} items, showing page {data['page']} of {data['pages']}")
                    if data['items']:
                        print(f"Sample: {data['items'][0]['title'][:100]}...")
                elif isinstance(data, list):
                    print(f"Found {len(data)} items")
                    if data:
                        print(f"Sample: {json.dumps(data[0], indent=2, default=str)}")
                else:
                    print(f"Response: {json.dumps(data, indent=2, default=str)}")
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
    print("🚀 Testing Simple News API")
    print(f"Base URL: {BASE_URL}")
    
    # Health check
    test_endpoint("GET", "/health", "Health Check")
    
    # Companies
    test_endpoint("GET", "/companies", "Get All Companies")
    test_endpoint("GET", "/companies/sectors", "Get All Sectors")
    test_endpoint("GET", "/companies/AAPL", "Get AAPL Company Details")
    test_endpoint("GET", "/companies/INVALID", "Get Invalid Company", expected_status=404)
    
    print(f"\n{'#'*60}")
    print("📰 TESTING NEWS ENDPOINTS")
    print("#"*60)
    
    # Company News (default: last day)
    test_endpoint("GET", "/news/company", "AAPL News (Last Day)", params={"c": "AAPL"})
    test_endpoint("GET", "/news/company", "AAPL News (Last 7 Days)", params={"c": "AAPL", "days": 7})
    test_endpoint("GET", "/news/company", "AAPL News (Limit 5)", params={"c": "AAPL", "days": 7, "limit": 5})
    test_endpoint("GET", "/news/company", "Invalid Company", params={"c": "INVALID"}, expected_status=404)
    
    # Sector News (default: last day)
    test_endpoint("GET", "/news/sector", "IT Sector News (Last Day)", params={"s": "Information Technology"})
    test_endpoint("GET", "/news/sector", "IT Sector News (Last 7 Days)", params={"s": "Information Technology", "days": 7})
    test_endpoint("GET", "/news/sector", "IT Sector News (Limit 3)", params={"s": "Information Technology", "days": 7, "limit": 3})
    test_endpoint("GET", "/news/sector", "Invalid Sector", params={"s": "Invalid Sector"}, expected_status=404)
    
    # Management
    test_endpoint("POST", "/refresh/AAPL", "Refresh AAPL")
    test_endpoint("POST", "/refresh/all", "Refresh All")
    test_endpoint("POST", "/refresh/INVALID", "Refresh Invalid Company", expected_status=404)
    
    # Debug
    test_endpoint("GET", "/debug/collect", "Debug Collect News")
    
    print(f"\n{'='*60}")
    print("🎉 API Test Complete!")
    print("\n📋 API Summary:")
    print("✅ GET /news/company?c=AAPL - Get Apple news (default: last day)")
    print("✅ GET /news/company?c=AAPL&days=7 - Get Apple news from last 7 days")
    print("✅ GET /news/sector?s=Information Technology - Get IT sector news")
    print("✅ Clean, simple API with no sentiment complexity")

if __name__ == "__main__":
    main()