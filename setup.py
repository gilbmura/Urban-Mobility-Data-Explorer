#!/usr/bin/env python3
"""
Setup script for NYC Mobility Data Explorer
"""

import os
import sys
import subprocess

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed: {e.stderr}")
        return False

def main():
    print("Setting up NYC Mobility Data Explorer...")
    
    # Check if we're in the right directory
    if not os.path.exists("backend/app.py"):
        print("Error: Please run this script from the project root directory")
        sys.exit(1)
    
    # Install Python dependencies
    if not run_command("pip install -r requirements.txt", "Installing Python dependencies"):
        print("Failed to install dependencies. Please check your Python environment.")
        sys.exit(1)
    
    print("\nSetup completed!")
    print("\nNext steps:")
    print("1. Set up your MySQL database (see README.md)")
    print("2. Load data: python scripts/simple_loader.py --csv data/processed/cleaned.csv")
    print("3. Start API: cd backend && python app.py")

if __name__ == "__main__":
    main()
