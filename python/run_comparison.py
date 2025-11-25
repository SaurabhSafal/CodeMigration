#!/usr/bin/env python3
"""
run_comparison.py

Easy-to-use script to compare tables between SQL Server and PostgreSQL.
This script provides a menu to choose which comparison tool to run.
"""

import sys
import os

# Add the current directory to path to ensure db_config can be imported
sys.path.insert(0, os.path.dirname(__file__))

from db_config import print_header

def main():
    print_header("Table Comparison Tool - SQL Server vs PostgreSQL")
    
    print("\nAvailable comparison tools:")
    print("  1) Simple Side-by-Side Comparison (comparetable.py)")
    print("     - Shows columns from both tables side by side")
    print("     - Easy visual comparison")
    print()
    print("  2) Powerful Auto-Mapping Comparison (compare_tables_powerful_auto_mapping.py)")
    print("     - Automatically suggests column mappings between tables")
    print("     - Uses intelligent matching algorithms")
    print("     - Shows confidence scores for matches")
    print()
    print("  3) Fetch Single Table Data (fetchdata.py)")
    print("     - Export a single table to Excel")
    print("     - Works with either SQL Server or PostgreSQL")
    print()
    print("  4) List All Tables (list_all_tables.py)")
    print("     - View all tables in both databases")
    print()
    print("  5) Table Details (table_details.py)")
    print("     - Get detailed information about a specific table")
    print()
    print("  0) Exit")
    
    choice = input("\nEnter your choice (0-5): ").strip()
    
    if choice == "1":
        print("\n" + "=" * 80)
        print("Running: Simple Side-by-Side Comparison")
        print("=" * 80)
        import comparetable
    elif choice == "2":
        print("\n" + "=" * 80)
        print("Running: Powerful Auto-Mapping Comparison")
        print("=" * 80)
        import compare_tables_powerful_auto_mapping
        compare_tables_powerful_auto_mapping.main()
    elif choice == "3":
        print("\n" + "=" * 80)
        print("Running: Fetch Single Table Data")
        print("=" * 80)
        import fetchdata
        fetchdata.main()
    elif choice == "4":
        print("\n" + "=" * 80)
        print("Running: List All Tables")
        print("=" * 80)
        import list_all_tables
    elif choice == "5":
        print("\n" + "=" * 80)
        print("Running: Table Details")
        print("=" * 80)
        import table_details
    elif choice == "0":
        print("\nExiting...")
        return
    else:
        print("\n✗ Invalid choice. Please run the script again.")
        return

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to exit...")
