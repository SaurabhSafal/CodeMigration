using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class TypeOfCategoryMasterMigration : MigrationService
{
    private readonly ILogger<TypeOfCategoryMasterMigration> _logger;
    private MigrationLogger? _migrationLogger;
    // SQL Server: TBL_TypeOfCategory -> PostgreSQL: type_of_category_master
    protected override string SelectQuery => @"
        SELECT 
            id,
            CategoryType
        FROM TBL_TypeOfCategory";

    protected override string InsertQuery => @"
        INSERT INTO type_of_category_master (
            type_of_category_id,
            type_of_category_name,
            company_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @type_of_category_id,
            @type_of_category_name,
            @company_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public TypeOfCategoryMasterMigration(IConfiguration configuration, ILogger<TypeOfCategoryMasterMigration> logger) : base(configuration)
    {
        _logger = logger; }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // type_of_category_id
            "Direct", // type_of_category_name
            "Fixed: 1"  // company_id
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "type_of_category_master");
        _migrationLogger.LogInfo("Starting migration");

        Console.WriteLine("🚀 Starting TypeOfCategoryMaster migration...");
        Console.WriteLine($"📋 Executing query...");
        
        // Load all company IDs from company_master so we can insert each type_of_category row for every company
        var companyIds = new List<int>();
        using (var compCmd = new NpgsqlCommand("SELECT company_id FROM company_master", pgConn, transaction))
        {
            using var compReader = await compCmd.ExecuteReaderAsync();
            while (await compReader.ReadAsync())
            {
                if (!compReader.IsDBNull(0)) companyIds.Add(compReader.GetInt32(0));
            }
        }
        
        Console.WriteLine($"✓ Found {companyIds.Count} companies. Each type_of_category will be inserted for all companies.");

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        Console.WriteLine($"✓ Query executed. Processing records...");

        int insertedCount = 0;
        int skippedCount = 0;
        int totalReadCount = 0;

        while (await reader.ReadAsync())
        {
            totalReadCount++;
            if (totalReadCount == 1)
            {
                Console.WriteLine($"✓ Found records! Processing...");
            }
            
            if (totalReadCount % 10 == 0)
            {
                Console.WriteLine($"📊 Processed {totalReadCount} records so far... (Inserted: {insertedCount}, Skipped: {skippedCount})");
            }

            var sourceId = reader["id"];
            var categoryName = reader["CategoryType"];
            
            // For each source row, insert one row per company
            foreach (var companyId in companyIds)
            {
                try
                {
                    // Use OVERRIDING SYSTEM VALUE to explicitly set the identity column
                    var insertWithOverride = @"
                        INSERT INTO type_of_category_master (
                            type_of_category_id,
                            type_of_category_name,
                            company_id,
                            created_by,
                            created_date,
                            modified_by,
                            modified_date,
                            is_deleted,
                            deleted_by,
                            deleted_date
                        ) OVERRIDING SYSTEM VALUE VALUES (
                            @type_of_category_id,
                            @type_of_category_name,
                            @company_id,
                            @created_by,
                            @created_date,
                            @modified_by,
                            @modified_date,
                            @is_deleted,
                            @deleted_by,
                            @deleted_date
                        )
                        ON CONFLICT (type_of_category_id) DO NOTHING";
                    
                    using var pgCmd = new NpgsqlCommand(insertWithOverride, pgConn, transaction);

                    pgCmd.Parameters.AddWithValue("@type_of_category_id", sourceId ?? DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@type_of_category_name", categoryName ?? DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@company_id", companyId);
                    pgCmd.Parameters.AddWithValue("@created_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@created_date", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@is_deleted", false);
                    pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                    int result = await pgCmd.ExecuteNonQueryAsync();
                    if (result > 0) 
                    {
                        insertedCount++;
                    }
                    else
                    {
                        // ON CONFLICT DO NOTHING - record already exists
                        skippedCount++;
                        Console.WriteLine($"⚠️  Skipping duplicate: type_of_category_id={sourceId}, company_id={companyId}");
                    }
                }
                catch (PostgresException pgEx)
                {
                    skippedCount++;
                    Console.WriteLine($"❌ PostgreSQL error for id={sourceId}, company={companyId}: {pgEx.MessageText}");
                    if (pgEx.Detail != null) Console.WriteLine($"   Detail: {pgEx.Detail}");
                    continue;
                }
                catch (Exception ex)
                {
                    skippedCount++;
                    Console.WriteLine($"❌ Error migrating id={sourceId}, company={companyId}: {ex.Message}");
                    continue;
                }
            }
        }

        Console.WriteLine($"\n📊 Migration Summary:");
        Console.WriteLine($"   Total source records read: {totalReadCount}");
        Console.WriteLine($"   ✓ Successfully inserted rows: {insertedCount}");
        Console.WriteLine($"   ❌ Skipped (errors/duplicates): {skippedCount}");
        
        if (totalReadCount == 0)
        {
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_TypeOfCategory table!");
        }
        
        // Reset the identity sequence to the max ID to avoid conflicts with future inserts
        if (insertedCount > 0)
        {
            try
            {
                var resetSequenceQuery = @"
                    SELECT setval(
                        pg_get_serial_sequence('type_of_category_master', 'type_of_category_id'),
                        COALESCE((SELECT MAX(type_of_category_id) FROM type_of_category_master), 1),
                        true
                    )";
                
                using var seqCmd = new NpgsqlCommand(resetSequenceQuery, pgConn, transaction);
                var newSeqValue = await seqCmd.ExecuteScalarAsync();
                Console.WriteLine($"✓ Reset identity sequence to {newSeqValue}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  Warning: Failed to reset identity sequence: {ex.Message}");
            }
        }

        return insertedCount;
    }
}
