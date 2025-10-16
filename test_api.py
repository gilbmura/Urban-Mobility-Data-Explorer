#!/usr/bin/env python3
"""
Simple API test script
"""

import requests
import json

def test_api():
    """Test the API endpoints."""
    base_url = "http://127.0.0.1:5000"
    
    print("Testing NYC Mobility API...")
    
    # Test health endpoint
    try:
        response = requests.get(f"{base_url}/health")
        print(f"✓ Health check: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"X Health check failed: {e}")
        return
    
    # Test stats summary
    try:
        response = requests.get(f"{base_url}/stats/summary")
        print(f"✓ Stats summary: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  - Total trips: {data.get('trips', 'N/A')}")
        else:
            print(f"  - Error: {response.json()}")
    except Exception as e:
        print(f"X Stats summary failed: {e}")
    
    # Test trips endpoint
    try:
        response = requests.get(f"{base_url}/trips?limit=2")
        print(f"✓ Trips list: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  - Found {len(data)} trips")
        else:
            print(f"  - Error: {response.json()}")
    except Exception as e:
        print(f"X Trips list failed: {e}")
    
    print("\nAPI test completed!")

if __name__ == "__main__":
    test_api()
