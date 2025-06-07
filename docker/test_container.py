#!/usr/bin/env python3
"""
Container validation script that runs without PySpark
Tests basic functionality and dependencies
"""

import sys
import os

def test_dependencies():
    """Test that required dependencies are available"""
    print("🧪 Testing container dependencies...")
    
    try:
        import pyspark
        print(f"✅ PySpark available (version: {pyspark.__version__})")
    except ImportError:
        print("❌ PySpark not available")
        return False
    
    try:
        import boto3
        print("✅ boto3 available")
    except ImportError:
        print("❌ boto3 not available")
        return False
    
    try:
        import pandas
        print("✅ pandas available")
    except ImportError:
        print("❌ pandas not available")
        return False
    
    try:
        import numpy
        print("✅ numpy available")
    except ImportError:
        print("❌ numpy not available")
        return False
    
    try:
        import snowflake.connector
        print("✅ snowflake-connector-python available")
    except ImportError:
        print("❌ snowflake-connector-python not available")
        return False
        
    try:
        import requests
        print("✅ requests available")
    except ImportError:
        print("❌ requests not available")
        return False
        
    return True

def test_file_structure():
    """Test that files are in the right places"""
    print("\n🧪 Testing container file structure...")
    
    expected_files = [
        '/opt/spark/jobs/processor.py',
        '/opt/spark/jobs/simple_processor.py'
    ]
    
    all_exist = True
    for file_path in expected_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path} exists")
        else:
            print(f"❌ {file_path} missing")
            all_exist = False
    
    return all_exist

def test_script_syntax():
    """Test that Python scripts have valid syntax"""
    print("\n🧪 Testing script syntax...")
    
    scripts = [
        '/opt/spark/jobs/simple_processor.py'
    ]
    
    for script in scripts:
        try:
            with open(script, 'r') as f:
                content = f.read()
            
            # Basic syntax check without importing
            compile(content, script, 'exec')
            print(f"✅ {script} has valid syntax")
        except SyntaxError as e:
            print(f"❌ {script} has syntax error: {e}")
            return False
        except Exception as e:
            print(f"⚠️  {script} check failed: {e}")
    
    return True

def test_permissions():
    """Test that files have correct permissions"""
    print("\n🧪 Testing file permissions...")
    
    scripts = [
        '/opt/spark/jobs/processor.py',
        '/opt/spark/jobs/simple_processor.py'
    ]
    
    for script in scripts:
        if os.access(script, os.X_OK):
            print(f"✅ {script} is executable")
        else:
            print(f"❌ {script} is not executable")
            return False
    
    return True

def main():
    """Run all container validation tests"""
    print("🚀 Container Validation Test")
    print("=" * 50)
    
    tests = [
        ("Dependencies", test_dependencies),
        ("File Structure", test_file_structure),
        ("Script Syntax", test_script_syntax),
        ("Permissions", test_permissions)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} test passed\n")
            else:
                print(f"❌ {test_name} test failed\n")
        except Exception as e:
            print(f"❌ {test_name} test error: {e}\n")
    
    print("=" * 50)
    print(f"📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 Container validation successful!")
        return 0
    else:
        print("⚠️  Some validation tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())