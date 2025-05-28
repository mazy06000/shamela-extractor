#!/usr/bin/env python3
"""
Clean Files Script for Shamela Pipeline
Deletes old CSV files from exported_indices/book_data directory
"""

import os
import shutil
from pathlib import Path
from datetime import datetime

def main():
    """Main function to clean old CSV files"""
    print("SHAMELA PIPELINE - CLEAN FILES")
    print("=" * 40)
    
    # Get the book_data directory path
    current_dir = Path.cwd()
    book_data_dir = current_dir / "exported_indices" / "book_data"
    
    # Check if the directory exists
    if not book_data_dir.exists():
        print(f"book_data directory not found: {book_data_dir}")
        print("Nothing to clean.")
        return
    
    # Count existing files
    csv_files = list(book_data_dir.glob("*.csv"))
    total_files = len(csv_files)
    
    if total_files == 0:
        print(f"book_data directory exists but contains no CSV files.")
        print("Nothing to clean.")
        return
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in csv_files)
    size_mb = total_size / (1024 * 1024)
    size_gb = total_size / (1024 * 1024 * 1024)
    
    # Show current status
    print(f"Found book_data directory: {book_data_dir}")
    print(f"Current contents:")
    print(f"   • {total_files:,} CSV files")
    if size_gb >= 1:
        print(f"   • {size_gb:.2f} GB total size")
    else:
        print(f"   • {size_mb:.1f} MB total size")
    
    print()
    
    # First confirmation
    response1 = input("Do you want to delete all old CSV files? (y/n): ").strip().lower()
    
    if response1 not in ['y', 'yes']:
        print("Operation cancelled.")
        return
    
    print()
    print("WARNING: This will permanently delete the entire book_data folder!")
    print(f"WARNING: {total_files:,} files will be lost and cannot be recovered (must run extract_indices again)!")
    print()
    
    # Second confirmation
    response2 = input("Are you sure? (y/n): ").strip().lower()
    
    if response2 not in ['y', 'yes']:
        print("Operation cancelled.")
        return
    
    # Perform deletion
    print()
    print("Deleting book_data folder...")
    
    try:
        # Record start time
        start_time = datetime.now()
        
        # Delete the entire directory
        shutil.rmtree(book_data_dir)
        
        # Calculate duration
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Success message
        print("Successfully deleted book_data folder!")
        print(f"Cleanup summary:")
        print(f"   • Deleted: {total_files:,} CSV files")
        if size_gb >= 1:
            print(f"   • Freed: {size_gb:.2f} GB disk space")
        else:
            print(f"   • Freed: {size_mb:.1f} MB disk space")
        print(f"   • Duration: {duration}")
        print()
        print("Run extract_indices.py to regenerate CSV files with updated format.")
        
    except PermissionError as e:
        print(f"Permission denied: {e}")
        print("Try running as administrator/sudo, or close any programs using these files.")
        
    except Exception as e:
        print(f"Error during deletion: {e}")
        print("Some files may still exist. Check the directory manually.")

if __name__ == "__main__":
    main()