# 🚀 Python Table Comparison Commands

## ✅ Configuration
Your database connections are now automatically loaded from `appsettings.json`!

**Current Configuration:**
- **SQL Server:** 192.168.3.250,1433 / predebug
- **PostgreSQL:** navikaran21112025.cpai22qoqeyo.ap-south-1.rds.amazonaws.com / navikaran_mig

---

## 🧪 Test Connections

```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\db_config.py
```

---

## 🎯 Run Table Comparisons

### 1️⃣ Menu Interface (EASIEST - Recommended)
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\run_comparison.py
```
Choose from menu options to run any tool.

---

### 2️⃣ Auto-Mapping Comparison (BEST for Migrations)
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\compare_tables_powerful_auto_mapping.py
```

**What it does:**
- Intelligently matches columns between SQL Server and PostgreSQL tables
- Provides confidence scores (0.0 - 1.0) for each match
- Creates Excel file with:
  - **Comparison sheet:** Side-by-side column view
  - **AutoMapping sheet:** Suggested column mappings with scores

**When prompted, enter:**
```
Enter MSSQL table name: TBL_UserMaster
Enter PostgreSQL table name: users
```
Or with schema:
```
Enter MSSQL table name: dbo.TBL_UserMaster
Enter PostgreSQL table name: public.users
```

---

### 3️⃣ Simple Side-by-Side Comparison
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\comparetable.py
```

**What it does:**
- Shows columns from both tables side by side
- Good for quick visual comparison

---

### 4️⃣ Export Single Table to Excel
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\fetchdata.py
```

**What it does:**
- Exports complete table data to Excel
- Choose SQL Server or PostgreSQL
- Supports row limits for large tables

---

### 5️⃣ List All Tables
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\list_all_tables.py
```

**What it does:**
- Shows all tables in both databases
- Helps you find table names

---

### 6️⃣ Get Table Details
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\table_details.py
```

**What it does:**
- Shows detailed column information for a specific table
- Includes data types, nullability, constraints, etc.

---

## 📂 View Output Files

```powershell
explorer d:\CodeMigration\python\migration_outputs
```

All Excel files are saved here.

---

## 📊 Understanding Auto-Mapping Results

The **AutoMapping** sheet in the Excel file shows:

| Column | Description |
|--------|-------------|
| **PG_COLUMN_NAME** | PostgreSQL column name |
| **MSSQL_COLUMN_NAME** | Matched SQL Server column (or "-" if no match) |
| **SUGGESTED_SCORE** | Confidence score (0.0 to 1.0) |
| **NOTES** | Interpretation of the match quality |

**Score Interpretation:**
- **≥ 0.95** = High confidence match (very likely correct) ✅
- **0.75-0.94** = Probable match (review recommended) ⚠️
- **0.35-0.74** = Low confidence (manual review needed) ⚠️⚠️
- **"-"** = No good match found ❌

---

## 🎯 Example: Compare Two Tables

```powershell
# Run the auto-mapping tool
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\compare_tables_powerful_auto_mapping.py
```

**Example Input:**
```
Enter MSSQL table name (or schema.table): TBL_usermasterfinal
Enter PostgreSQL table name (table only or schema.table): users
```

**Output:**
- Excel file created: `Compare_TBL_usermasterfinal_vs_users_powerful_mapping_fixed.xlsx`
- Location: `d:\CodeMigration\python\migration_outputs\`
- Contains both comparison and auto-mapping sheets

---

## 🔧 Quick Tips

1. **No need to edit db_config.py** - it reads from appsettings.json automatically
2. **Use auto-mapping** for migration planning - it saves hours of manual work
3. **Schema names are optional** - defaults to `dbo` for SQL Server, `public` for PostgreSQL
4. **All outputs go to migration_outputs folder** - easy to find and review

---

## 🚀 Quick Start (Copy & Paste)

**Test connections:**
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\db_config.py
```

**Run menu interface:**
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\run_comparison.py
```

**Or run auto-mapping directly:**
```powershell
D:/CodeMigration/.venv/Scripts/python.exe d:\CodeMigration\python\compare_tables_powerful_auto_mapping.py
```

---

## 📞 Need Help?

- Check console output for detailed error messages
- Verify both databases are accessible
- Ensure table names are spelled correctly
- Schema names: SQL Server uses `dbo`, PostgreSQL uses `public`
