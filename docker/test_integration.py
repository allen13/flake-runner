#!/usr/bin/env python3
"""
Integration test for simple_processor.py
Tests local file system processing capabilities
Lightweight test that doesn't require external dependencies
"""

import os
import sys
import subprocess
import tempfile
import shutil
import json
import csv
from pathlib import Path

# Add the src directory to the path so we can import the processor
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

class TestSimpleProcessor:
    """Integration tests for SimpleProcessor"""
    
    def create_temp_dir(self):
        """Create a temporary directory for test files"""
        return tempfile.mkdtemp()
    
    def create_sample_csv_data(self, temp_dir):
        """Create sample CSV test data"""
        csv_path = os.path.join(temp_dir, "test_input.csv")
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'value', 'active'])
            writer.writerow([1, 'Alice', 10.5, True])
            writer.writerow([2, 'Bob', 20.3, False])
            writer.writerow([3, 'Charlie', 30.1, True])
            writer.writerow([4, 'Diana', 40.7, True])
            writer.writerow([5, 'Eve', 50.9, False])
        return csv_path
    
    def create_sample_json_data(self, temp_dir):
        """Create sample JSON test data"""
        json_path = os.path.join(temp_dir, "test_input.json")
        data = [
            {'id': 1, 'name': 'Alice', 'value': 10.5, 'active': True},
            {'id': 2, 'name': 'Bob', 'value': 20.3, 'active': False},
            {'id': 3, 'name': 'Charlie', 'value': 30.1, 'active': True},
            {'id': 4, 'name': 'Diana', 'value': 40.7, 'active': True},
            {'id': 5, 'name': 'Eve', 'value': 50.9, 'active': False}
        ]
        with open(json_path, 'w') as f:
            json.dump(data, f)
        return json_path
    
    def test_csv_processing(self):
        """Test CSV file processing"""
        temp_dir = self.create_temp_dir()
        
        try:
            sample_csv_data = self.create_sample_csv_data(temp_dir)
            output_path = os.path.join(temp_dir, "output_csv")
            
            # Run the simple processor
            result = subprocess.run([
                sys.executable, 
                os.path.join(os.path.dirname(__file__), 'src', 'simple_processor.py'),
                '--input-path', sample_csv_data,
                '--output-path', output_path,
                '--input-format', 'CSV',
                '--output-format', 'CSV',  # Use CSV output to avoid parquet dependencies
                '--job-id', 'test-csv-job'
            ], capture_output=True, text=True)
            
            # Check that the process completed successfully
            if result.returncode != 0:
                print(f"❌ Process failed with error: {result.stderr}")
                print(f"📤 Output: {result.stdout}")
                return False
                
            # Check that output files were created
            if not os.path.exists(output_path):
                print("❌ Output directory was not created")
                return False
            
            # Check that CSV files were created
            csv_files = list(Path(output_path).glob("*.csv"))
            if len(csv_files) == 0:
                print("❌ No CSV files were created")
                return False
            
            print("✅ CSV processing test passed")
            return True
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_json_processing(self, temp_dir, sample_json_data):
        """Test JSON file processing"""
        output_path = os.path.join(temp_dir, "output_json")
        
        # Run the simple processor
        result = subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), 'src', 'simple_processor.py'),
            '--input-path', sample_json_data,
            '--output-path', output_path,
            '--input-format', 'JSON',
            '--output-format', 'CSV',
            '--job-id', 'test-json-job'
        ], capture_output=True, text=True)
        
        # Check that the process completed successfully
        assert result.returncode == 0, f"Process failed with error: {result.stderr}"
        
        # Check that output files were created
        assert os.path.exists(output_path), "Output directory was not created"
        
        # Check that CSV files were created
        csv_files = list(Path(output_path).glob("*.csv"))
        assert len(csv_files) > 0, "No CSV files were created"
        
        # Verify the content by reading it back
        # Spark writes CSV files in parts, so we need to read all parts
        all_data = []
        for csv_file in csv_files:
            if csv_file.name.startswith('part-'):
                df_part = pd.read_csv(csv_file)
                all_data.append(df_part)
        
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
        else:
            # If no part files, try to read from a single CSV file
            df = pd.read_csv(csv_files[0])
        
        # Check that we have the expected number of records
        assert len(df) == 5, f"Expected 5 records, got {len(df)}"
        
        # Check that processing metadata was added
        assert '_processing_timestamp' in df.columns, "Processing timestamp not added"
        assert '_job_id' in df.columns, "Job ID not added"
        assert '_source_system' in df.columns, "Source system not added"
        
        print("✅ JSON processing test passed")
    
    def test_command_line_interface(self, temp_dir):
        """Test the command line interface with built-in test data"""
        # Use the sample CSV data we created
        input_path = os.path.join(os.path.dirname(__file__), 'test_data', 'sample_customers.csv')
        output_path = os.path.join(temp_dir, "cli_output")
        
        # Test with minimal arguments
        result = subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), 'src', 'simple_processor.py'),
            '--input-path', input_path,
            '--output-path', output_path
        ], capture_output=True, text=True)
        
        # Check that the process completed successfully
        assert result.returncode == 0, f"Process failed with error: {result.stderr}"
        
        # Check that output was created
        assert os.path.exists(output_path), "Output directory was not created"
        
        print("✅ Command line interface test passed")
    
    def test_error_handling(self, temp_dir):
        """Test error handling for missing input files"""
        non_existent_input = os.path.join(temp_dir, "non_existent.csv")
        output_path = os.path.join(temp_dir, "error_output")
        
        # Run the simple processor with non-existent input
        result = subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), 'src', 'simple_processor.py'),
            '--input-path', non_existent_input,
            '--output-path', output_path
        ], capture_output=True, text=True)
        
        # Check that the process failed as expected
        assert result.returncode != 0, "Process should have failed for non-existent input file"
        
        print("✅ Error handling test passed")

def run_manual_test():
    """Manual test function that can be run without pytest"""
    print("🚀 Running manual integration test for simple_processor.py")
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create test data
        test_csv = os.path.join(temp_dir, "manual_test.csv")
        with open(test_csv, 'w') as f:
            f.write("id,name,value\n")
            f.write("1,Test User,100.5\n")
            f.write("2,Another User,200.3\n")
        
        output_path = os.path.join(temp_dir, "manual_output")
        
        # Run the processor
        print(f"📁 Input: {test_csv}")
        print(f"📁 Output: {output_path}")
        
        result = subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), 'src', 'simple_processor.py'),
            '--input-path', test_csv,
            '--output-path', output_path,
            '--job-id', 'manual-test'
        ], capture_output=True, text=True)
        
        print(f"📊 Exit code: {result.returncode}")
        if result.stdout:
            print(f"📤 Output: {result.stdout}")
        if result.stderr:
            print(f"❌ Errors: {result.stderr}")
        
        # Check results
        if result.returncode == 0:
            print("✅ Manual test completed successfully")
            if os.path.exists(output_path):
                files = os.listdir(output_path)
                print(f"📄 Output files: {files}")
            else:
                print("⚠️  Output directory not found")
        else:
            print("❌ Manual test failed")
        
    finally:
        # Clean up
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    # Check if we should run pytest or manual test
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        run_manual_test()
    else:
        # Try to run with pytest if available
        try:
            import pytest
            print("🧪 Running integration tests with pytest...")
            pytest.main([__file__, "-v"])
        except ImportError:
            print("📦 pytest not available, running manual test...")
            run_manual_test()