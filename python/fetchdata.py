#!/usr/bin/env python3
"""
download_table_to_excel.py

Interactive utility to export a table from either MSSQL or PostgreSQL to an Excel file.

Flow:
 - Ask the user which connection to use (MSSQL or PostgreSQL)
 - Ask for table name (supports schema.table or plain table)
 - Verify the table exists in the chosen database (prompts if not)
 - Show row count and, if large, ask whether to download all rows or a limited sample
 - Download the data into a DataFrame and save it to an Excel file
 - Uses get_mssql_connection(), get_postgres_connection(), print_header(), get_output_path()
   from your db_config module.

Notes:
 - Table identifiers are validated to allow only letters/digits/underscore and an optional schema.
 - For safety, if row count is large (default threshold 100k) you'll be asked to confirm or specify a limit.
 - Requires pandas and openpyxl.
"""

import re
import sys
import traceback
from datetime import datetime

import pandas as pd
from db_config import (
    get_mssql_connection,
    get_postgres_connection,
    print_header,
    get_output_path,
)


# Config
LARGE_TABLE_THRESHOLD = 100_000  # warn if table has more rows than this


def valid_identifier(name: str) -> bool:
    """Allow schema.table or table where parts contain only alnum and underscore."""
    if not name or name.strip() == "":
        return False
    parts = name.split(".")
    if len(parts) > 2:
        return False
    for p in parts:
        if not re.match(r"^[A-Za-z0-9_]+$", p):
            return False
    return True


def parse_schema_table(name: str, default_schema: str) -> (str, str):
    """Return (schema, table) given input 'schema.table' or 'table'."""
    if "." in name:
        parts = name.split(".", 1)
        return parts[0].strip(), parts[1].strip()
    return default_schema, name.strip()


def table_exists_mssql(conn, schema: str, table: str) -> bool:
    q = f"""
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """
    try:
        df = pd.read_sql(q, conn)
        return not df.empty
    except Exception:
        return False


def table_exists_postgres(conn, schema: str, table: str) -> bool:
    q = f"""
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    try:
        df = pd.read_sql(q, conn)
        return not df.empty
    except Exception:
        return False


def get_row_count(conn, db_type: str, schema: str, table: str) -> int:
    if db_type == "mssql":
        q = f"SELECT COUNT(*) AS cnt FROM [{schema}].[{table}]"
    else:
        # postgres
        q = f"SELECT COUNT(*) AS cnt FROM \"{schema}\".\"{table}\""
    try:
        df = pd.read_sql(q, conn)
        if not df.empty:
            return int(df.iloc[0]["cnt"])
    except Exception:
        pass
    return -1


def build_select_query(db_type: str, schema: str, table: str, limit: int = None) -> str:
    if db_type == "mssql":
        base = f"SELECT * FROM [{schema}].[{table}]"
        if limit and limit > 0:
            return f"SELECT TOP {limit} * FROM [{schema}].[{table}]"
        return base
    else:
        # postgres - use LIMIT
        base = f"SELECT * FROM \"{schema}\".\"{table}\""
        if limit and limit > 0:
            return f"{base} LIMIT {limit}"
        return base


def main():
    print_header("Export Table to Excel")

    # choose connection
    print("\nChoose connection to export from:")
    print("  1) MSSQL")
    print("  2) PostgreSQL")
    choice = input("Enter choice (1 or 2): ").strip()
    if choice not in ("1", "2"):
        print("Invalid choice. Exiting.")
        return

    db_type = "mssql" if choice == "1" else "postgres"
    conn = None

    try:
        if db_type == "mssql":
            print("\n[1/3] Connecting to MSSQL...")
            conn = get_mssql_connection()
            default_schema = "dbo"
            print("✓ MSSQL Connected")
        else:
            print("\n[1/3] Connecting to PostgreSQL...")
            conn = get_postgres_connection()
            default_schema = "public"
            print("✓ PostgreSQL Connected")

        # ask for table name
        print("\nEnter the table to export.")
        print(" - You may enter 'schema.table' or just 'table' (will use default schema).")
        table_input = input("Table name: ").strip()
        if not valid_identifier(table_input):
            print("Invalid table name format. Only letters, digits and underscore are allowed (optionally schema.table).")
            return

        schema, table = parse_schema_table(table_input, default_schema)

        # verify existence
        print("\nChecking table existence...")
        exists = False
        if db_type == "mssql":
            exists = table_exists_mssql(conn, schema, table)
        else:
            exists = table_exists_postgres(conn, schema, table)

        if not exists:
            print(f"✗ Table '{schema}.{table}' not found in {db_type.upper()} (or not visible).")
            return

        print(f"✓ Found table: {schema}.{table}")

        # row count
        print("\nGetting row count (may take a moment)...")
        row_count = get_row_count(conn, db_type, schema, table)
        if row_count == -1:
            print("Could not determine row count. Proceeding without row-count guard.")
        else:
            print(f"Row count: {row_count:,}")

        # handle large tables
        limit = None
        if row_count == -1:
            # unknown, ask user whether download all or sample
            ans = input("Row count unknown. Download all rows? (y/N) ").strip().lower()
            if ans != "y":
                n = input("Enter row limit to download (e.g. 1000) or press Enter to cancel: ").strip()
                if not n:
                    print("Cancelled.")
                    return
                try:
                    limit = int(n)
                except ValueError:
                    print("Invalid number. Exiting.")
                    return
        else:
            if row_count > LARGE_TABLE_THRESHOLD:
                print(f"Warning: table has more than {LARGE_TABLE_THRESHOLD:,} rows.")
                print("Downloading very large tables may take a long time and produce a very large Excel file.")
                ans = input("Do you want to (A)ll rows, (L)imit rows, or (C)ancel? [L] ").strip().lower()
                if ans == "a":
                    limit = None
                elif ans == "c" or ans == "":
                    print("Cancelled.")
                    return
                else:
                    n = input("Enter row limit to download (e.g. 10000): ").strip()
                    try:
                        limit = int(n)
                    except ValueError:
                        print("Invalid number. Exiting.")
                        return

        # build and run query
        print("\nFetching data...")
        query = build_select_query(db_type, schema, table, limit=limit)
        # Use pandas to read query
        # Note: pandas may warn if connection is not SQLAlchemy engine; that's fine.
        df = pd.read_sql(query, conn)
        print(f"✓ Retrieved {len(df):,} rows and {len(df.columns)} columns.")

        # prepare output path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_table_name = f"{schema}_{table}".replace(" ", "_")
        output_file = get_output_path(f"export_{db_type}_{safe_table_name}_{timestamp}.xlsx")
        print(f"\nSaving to: {output_file}")

        # Write to Excel
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Write the data sheet
            sheet_name = table if len(table) <= 31 else table[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            # Optionally add a small metadata sheet
            meta = {
                "exported_at": [datetime.now().isoformat()],
                "source_db": [db_type],
                "schema": [schema],
                "table": [table],
                "rows_exported": [len(df)],
            }
            pd.DataFrame(meta).to_excel(writer, sheet_name="__metadata", index=False)

        print("✓ Export complete.")
        print(f"File saved: {output_file}")

    except Exception as e:
        print("\n✗ Export failed!")
        print("Error:", str(e))
        traceback.print_exc()
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()