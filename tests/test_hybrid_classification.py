#!/usr/bin/env python3
"""
Test script for hybrid ML classification system
Run this after implementing the hybrid classifier
"""

import requests
import json
import time

# Base URL for your API
BASE_URL = "http://localhost:8000/api"

def test_endpoint(method, endpoint, description, expected_status=200, params=None):
    """Test a single endpoint and print results"""
    url = f"{BASE_URL}{endpoint}"
    
    print(f"\n{'='*60}")
    print(f"🧪 Testing: {description}")
    print(f"URL: {url}")
    if params:
        print(f"Params: {params}")
    
    try:
        start_time = time.time()
        
        if method.upper() == "GET":
            response = requests.get(url, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, json=params)
        
        end_time = time.time()
        response_time = (end_time - start_time) * 1000  # Convert to ms
        
        print(f"Status: {response.status_code} | Response Time: {response_time:.1f}ms")
        
        if response.status_code == expected_status:
            print("✅ PASS")
            
            try:
                data = response.json()
                if isinstance(data, dict) and 'items' in data:
                    print(f"📊 Found {data['total']} items, showing page {data['page']} of {data['pages']}")
                    
                    # Show detailed analysis for news items
                    if data['items'] and 'company_ml_scores' in data['items'][0]:
                        print("\n📈 ML Classification Analysis:")
                        for i, item in enumerate(data['items'][:3]):  # Show first 3
                            print(f"\n📰 Article {i+1}: {item['title'][:80]}...")
                            ml_scores = item.get('company_ml_scores', {})
                            if ml_scores:
                                for company_id, scores in ml_scores.items():
                                    print(f"   🏢 Company ID {company_id}:")
                                    print(f"      • Relevance: {scores['relevance_score']:.3f}")
                                    print(f"      • Linguistic: {scores['linguistic_score']:.3f}")
                                    print(f"      • ML Score: {scores['ml_score']:.3f}")
                                    print(f"      • Confidence: {scores['confidence']:.3f}")
                            else:
                                print("   ❌ No companies identified")
                    
                    elif data['items']:
                        print(f"📄 Sample: {data['items'][0]['title'][:80]}...")
                        
                elif isinstance(data, list):
                    print(f"📋 Found {len(data)} items")
                    if data:
                        print(f"📄 Sample: {json.dumps(data[0], indent=2, default=str)}")
                else:
                    print(f"📄 Response: {json.dumps(data, indent=2, default=str)}")
            except:
                print(f"📄 Response: {response.text}")
        else:
            print("❌ FAIL")
            print(f"Expected: {expected_status}, Got: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

def test_ml_classification_quality():
    """Test the quality of ML classification"""
    print(f"\n{'#'*60}")
    print("🔬 TESTING ML CLASSIFICATION QUALITY")
    print("#"*60)
    
    # Test cases with expected good/bad classifications
    test_cases = [
        {
            "title": "Apple reports record Q4 iPhone sales beating analyst expectations",
            "expected": "AAPL",
            "should_find": True
        },
        {
            "title": "Google unveils new AI features for search and advertising",
            "expected": "GOOGL", 
            "should_find": True
        },
        {
            "title": "Microsoft Azure cloud revenue grows 30% in latest quarter",
            "expected": "MSFT",
            "should_find": True
        },
        {
            "title": "Local apple orchard opens for fall harvest season",
            "expected": "AAPL",
            "should_find": False  # Should NOT classify as Apple Inc.
        },
        {
            "title": "How to google search more effectively for students",
            "expected": "GOOGL",
            "should_find": False  # Should NOT classify as Google Inc.
        }
    ]
    
    print("🧪 Running classification quality tests...")
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- Test Case {i} ---")
        print(f"Title: {test_case['title']}")
        print(f"Expected: {test_case['expected']} | Should Find: {test_case['should_find']}")
        
        # This would require a direct API endpoint to test classification
        # For now, we'll just document the test cases
        print("📝 Manual verification needed")

def main():
    """Run comprehensive hybrid ML tests"""
    print("🚀 Testing Hybrid ML Classification System")
    print(f"Base URL: {BASE_URL}")
    
    # Health check
    test_endpoint("GET", "/health", "Health Check")
    
    # Test debug collection to see new classification in action
    print(f"\n{'#'*60}")
    print("🔄 TESTING NEWS COLLECTION WITH HYBRID ML")
    print("#"*60)
    
    test_endpoint("GET", "/debug/collect", "Debug Collect with Hybrid ML")
    
    # Test news endpoints to see ML scores
    print(f"\n{'#'*60}")
    print("📊 TESTING NEWS ENDPOINTS WITH ML SCORES")
    print("#"*60)
    
    # Company news with detailed ML analysis
    test_endpoint("GET", "/news/company", "AAPL News with ML Scores", params={"c": "AAPL", "days": 7, "limit": 5})
    test_endpoint("GET", "/news/company", "GOOGL News with ML Scores", params={"c": "GOOGL", "days": 7, "limit": 5})
    test_endpoint("GET", "/news/company", "MSFT News with ML Scores", params={"c": "MSFT", "days": 7, "limit": 5})
    
    # Sector news
    test_endpoint("GET", "/news/sector", "IT Sector News with ML", params={"s": "Information Technology", "days": 7, "limit": 5})
    
    # Test classification quality
    test_ml_classification_quality()
    
    # Performance summary
    print(f"\n{'='*60}")
    print("🎯 HYBRID ML TEST SUMMARY")
    print("="*60)
    print("\n✅ Expected Improvements:")
    print("   • More accurate company classification")
    print("   • Detailed scoring (linguistic + ML + confidence)")
    print("   • Fewer false positives (especially for GOOGL)")
    print("   • Better handling of ambiguous company names")
    
    print("\n📊 What to Look For:")
    print("   • Response times should be reasonable (<100ms per article)")
    print("   • ML scores between 0.0-1.0")
    print("   • Higher confidence for clearly relevant articles")
    print("   • Exclusion of false positives")
    
    print("\n🔧 Tuning Options:")
    print("   • Adjust min_relevance threshold (currently 0.6)")
    print("   • Modify score weights (linguistic vs ML)")
    print("   • Add custom exclusion patterns")
    
    print("\n🎉 Hybrid ML Implementation Test Complete!")

if __name__ == "__main__":
    main()