// Optimized UsersMasterMigration service

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Helper;
using Helpers;
using System.Diagnostics;
using System.Threading;
using DataMigration.Services;
using System.Data;
using System.Collections.Concurrent;
using NpgsqlTypes;

public class UsersMasterMigration : MigrationService
{
    private readonly AesEncryptionService _aesEncryptionService;
    private readonly IConfiguration _configuration;
    private readonly bool _fastMode;
    private readonly int _hashIterations;
    private readonly int _transformWorkerCount;
    private readonly int _rawQueueCapacity;
    private readonly int _writeQueueCapacity;
    private const int INITIAL_BATCH_SIZE = 1000;
    private const int PROGRESS_UPDATE_INTERVAL = 100;

    protected override string SelectQuery => @"
        SELECT 
            PERSON_ID, USER_ID, USERPASSWORD, FULL_NAME, EMAIL_ADDRESS, MobileNumber, STATUS,REPORTINGTO,
            USERTYPE_ID, CURRENCYID, TIMEZONE, USER_SAP_ID, DEPARTMENTHEAD, DigitalSignature
        FROM TBL_USERMASTERFINAL";

    protected override string InsertQuery => @"
        INSERT INTO users (
            user_id, username, password_hash, full_name, email, mobile_number, status,
            password_salt, masked_email, masked_mobile_number, email_hash, mobile_hash, failed_login_attempts,
            last_failed_login, lockout_end, last_login_date, is_mfa_enabled, mfa_type, mfa_secret, last_mfa_sent_at,
            reporting_to_id, lockout_count, azureoid, user_type, currency, location, client_sap_code,
            digital_signature, last_password_changed, is_active, created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date, erp_username, approval_head, time_zone_country, digital_signature_path
        ) VALUES (
            @user_id, @username, @password_hash, @full_name, @email, @mobile_number, @status,
            @password_salt, @masked_email, @masked_mobile_number, @email_hash, @mobile_hash, @failed_login_attempts,
            @last_failed_login, @lockout_end, @last_login_date, @is_mfa_enabled, @mfa_type, @mfa_secret, @last_mfa_sent_at,
            @reporting_to_id, @lockout_count, @azureoid, @user_type, @currency, @location, @client_sap_code,
            @digital_signature, @last_password_changed, @is_active, @created_by, @created_date, @modified_by, @modified_date,
            @is_deleted, @deleted_by, @deleted_date, @erp_username, @approval_head, @time_zone_country, @digital_signature_path
        )";

    public UsersMasterMigration(IConfiguration configuration) : base(configuration)
    {
        _configuration = configuration;
        _aesEncryptionService = new AesEncryptionService();

        // Read migration tweaks from configuration; use fast defaults for faster migration
        _fastMode = _configuration.GetValue<bool?>("Migration:FastMode") ?? true;
        _hashIterations = _configuration.GetValue<int?>("Migration:Users:HashIterations") ?? (_fastMode ? 10000 : 1500000);
        _transformWorkerCount = _configuration.GetValue<int?>("Migration:Users:TransformWorkerCount") ?? Math.Max(1, Environment.ProcessorCount - 1);
        _rawQueueCapacity = _configuration.GetValue<int?>("Migration:Users:RawQueueCapacity") ?? Math.Max(1000, _transformWorkerCount * 2000);
        _writeQueueCapacity = _configuration.GetValue<int?>("Migration:Users:WriteQueueCapacity") ?? Math.Max(5000, _transformWorkerCount * 2000);
    }

    private async Task<int> GetTotalRecordsAsync(SqlConnection sqlConn)
    {
        using var cmd = new SqlCommand("SELECT COUNT(*) FROM TBL_USERMASTERFINAL", sqlConn);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<int> MigrateAsync(IMigrationProgress? progress = null)
    {
        progress ??= new ConsoleMigrationProgress();
        SqlConnection? sqlConn = null;
        NpgsqlConnection? pgConn = null;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            sqlConn = GetSqlServerConnection();
            pgConn = GetPostgreSqlConnection();
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            progress.ReportProgress(0, 0, "Estimating total records...", stopwatch.Elapsed);
            int totalRecords = await GetTotalRecordsAsync(sqlConn);

            using var transaction = await pgConn.BeginTransactionAsync();
            try
            {
                int result = await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
                await transaction.CommitAsync();
                return result;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                progress.ReportError($"Transaction rolled back due to error: {ex.Message}", 0);
                throw;
            }
        }
        finally
        {
            sqlConn?.Dispose();
            pgConn?.Dispose();
        }
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        var progress = new ConsoleMigrationProgress();
        var stopwatch = Stopwatch.StartNew();
        int totalRecords = await GetTotalRecordsAsync(sqlConn);
        return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
    }

    private class RawUserRow
    {
        public int PersonId { get; set; }
        public string UserId { get; set; } = "";
        public string RawPassword { get; set; } = "";
        public string FullName { get; set; } = "";
        public string Email { get; set; } = "";
        public string MobileNumber { get; set; } = "";
        public string Status { get; set; } = "";
        public string ReportingTo { get; set; } = "";
        public string UserType { get; set; } = "";
        public string Currency { get; set; } = "";
        public string Location { get; set; } = "";
        public string ErpUsername { get; set; } = "";
        public string ApprovalHead { get; set; } = "";
        public string DigitalSignature { get; set; } = "";
    }

    private UserRecord? BuildUserRecordFromRaw(RawUserRow raw)
    {
        try
        {
            // use configured hash iterations for speed or security based on _fastMode
            var (passwordHash, passwordSalt) = PasswordEncryptionHelper.EncryptPassword(raw.RawPassword ?? string.Empty, _hashIterations);
            var encryptedEmail = string.IsNullOrEmpty(raw.Email) ? string.Empty : _aesEncryptionService.Encrypt(raw.Email);
            var encryptedMobileNumber = string.IsNullOrEmpty(raw.MobileNumber) ? string.Empty : _aesEncryptionService.Encrypt(raw.MobileNumber);
            var emailHash = string.IsNullOrEmpty(raw.Email) ? string.Empty : AesEncryptionService.ComputeSha256Hash(raw.Email);
            var mobileHash = string.IsNullOrEmpty(raw.MobileNumber) ? string.Empty : AesEncryptionService.ComputeSha256Hash(raw.MobileNumber);

            return new UserRecord
            {
                UserId = raw.PersonId,
                Username = raw.UserId ?? "",
                PasswordHash = passwordHash,
                PasswordSalt = passwordSalt,
                FullName = raw.FullName ?? "",
                Email = encryptedEmail,
                MobileNumber = encryptedMobileNumber,
                Status = raw.Status ?? "",
                MaskedEmail = MaskHelper.MaskEmail(raw.Email ?? string.Empty),
                MaskedMobileNumber = MaskHelper.MaskPhoneNumber(raw.MobileNumber ?? string.Empty),
                EmailHash = emailHash,
                MobileHash = mobileHash,
                ReportingToId = raw.ReportingTo ?? "",
                UserType = raw.UserType ?? "",
                Currency = raw.Currency ?? "",
                Location = raw.Location ?? "",
                ErpUsername = raw.ErpUsername ?? "",
                ApprovalHead = raw.ApprovalHead ?? "",
                DigitalSignature = raw.DigitalSignature ?? "",
                DigitalSignaturePath = "/Documents/TechnicalDocuments/" + (raw.DigitalSignature ?? "")
            };
        }
        catch (Exception ex)
        {
            // log minimal info
            Console.WriteLine($"Transform error for user {raw.PersonId}: {ex.Message}");
            return null;
        }
    }

    private async Task<int> ExecuteOptimizedMigrationAsync(
        SqlConnection sqlConn,
        NpgsqlConnection pgConn,
        int totalRecords,
        IMigrationProgress progress,
        Stopwatch stopwatch,
        NpgsqlTransaction? transaction = null)
    {
        int insertedCount = 0;
        int processedCount = 0;
        int skippedCount = 0;
        int errorCount = 0;

        // Heuristic for worker count
        int transformWorkerCount = Math.Max(1, _transformWorkerCount);
        int rawQueueCapacity = Math.Max(1000, _rawQueueCapacity);
        const int TRANSFORM_BATCH_SIZE = 500; // number of user records each transform worker accumulates
        int writeQueueCapacity = Math.Max(4, rawQueueCapacity / TRANSFORM_BATCH_SIZE);

        var rawQueue = new BlockingCollection<RawUserRow>(rawQueueCapacity);
        var writeQueue = new BlockingCollection<List<UserRecord>>(writeQueueCapacity);
        var listPool = new ConcurrentBag<List<UserRecord>>();
        // Pre-seed pool to avoid allocations
        for (int i = 0; i < Math.Max(4, transformWorkerCount * 2); i++)
        {
            listPool.Add(new List<UserRecord>(TRANSFORM_BATCH_SIZE));
        }
        var cts = new CancellationTokenSource();
        var token = cts.Token;

        // Prepare COPY command once
        var copyCommand = @"COPY users (
            user_id, username, password_hash, full_name, email, mobile_number, status,
            password_salt, masked_email, masked_mobile_number, email_hash, mobile_hash, failed_login_attempts,
            last_failed_login, lockout_end, last_login_date, is_mfa_enabled, mfa_type, mfa_secret, last_mfa_sent_at,
            reporting_to_id, lockout_count, azureoid, user_type, currency, location, client_sap_code,
            digital_signature, last_password_changed, is_active, created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date, erp_username, approval_head, time_zone_country, digital_signature_path
        ) FROM STDIN (FORMAT BINARY)";

        Exception? backgroundException = null;

        // Consumer writer: write into PG using a single BinaryImport writer to minimize overhead
        var writerTask = Task.Run(async () =>
        {
            try
            {
                using var writer = await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None);
                var now = DateTime.UtcNow;

                foreach (var batch in writeQueue.GetConsumingEnumerable(token))
                {
                    try
                    {
                        foreach (var record in batch)
                        {
                            writer.StartRow();
                            writer.Write(record.UserId, NpgsqlTypes.NpgsqlDbType.Integer);
                            writer.Write(record.Username ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.PasswordHash ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.FullName ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.Email ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.MobileNumber ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.Status ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.PasswordSalt ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.MaskedEmail ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.MaskedMobileNumber ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.EmailHash ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.MobileHash ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(0, NpgsqlTypes.NpgsqlDbType.Integer); // failed_login_attempts
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_failed_login
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // lockout_end
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_login_date
                            writer.Write(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_mfa_enabled
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // mfa_type
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // mfa_secret
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_mfa_sent_at
                            writer.Write(record.ReportingToId ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(0, NpgsqlTypes.NpgsqlDbType.Integer); // lockout_count
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // azureoid
                            writer.Write(record.UserType ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.Currency ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.Location ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // client_sap_code
                            writer.Write(record.DigitalSignature ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_password_changed
                            writer.Write(true, NpgsqlTypes.NpgsqlDbType.Boolean); // is_active
                            writer.Write(0, NpgsqlTypes.NpgsqlDbType.Integer); // created_by
                            writer.Write(now, NpgsqlTypes.NpgsqlDbType.Timestamp); // created_date
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // modified_by
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // modified_date
                            writer.Write(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_deleted
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // deleted_by
                            writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // deleted_date
                            writer.Write(record.ErpUsername ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.ApprovalHead ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(record.Location ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text); // time_zone_country
                            writer.Write(record.DigitalSignaturePath ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                        }

                        Interlocked.Add(ref insertedCount, batch.Count);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);
                        progress.ReportError($"Error writing record chunk to PostgreSQL: {ex.Message}", processedCount);
                        // stop on write errors
                        cts.Cancel();
                        throw;
                    }
                }

                writer.Complete();
            }
            catch (Exception ex)
            {
                backgroundException = ex;
                cts.Cancel();
            }
        }, token);

        // Determine writer count
        int writerCount = transaction != null ? 1 : Math.Min(4, Math.Max(1, Environment.ProcessorCount / 2));

        var writerTasks = new List<Task>(writerCount);
        for (int w = 0; w < writerCount; w++)
        {
            writerTasks.Add(Task.Run(async () =>
            {
                NpgsqlConnection? writerConn = null;
                try
                {
                    bool isSharedConnection = transaction != null;
                    if (isSharedConnection)
                    {
                        writerConn = pgConn; // shared connection
                    }
                    else
                    {
                        writerConn = GetPostgreSqlConnection();
                        await writerConn.OpenAsync(token);
                    }

                    using var writer = await writerConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None);
                    var now = DateTime.UtcNow;

                    foreach (var batchList in writeQueue.GetConsumingEnumerable(token))
                    {
                        try
                        {
                            foreach (var record in batchList)
                            {
                                writer.StartRow();
                                writer.Write(record.UserId, NpgsqlTypes.NpgsqlDbType.Integer);
                                writer.Write(record.Username ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.PasswordHash ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.FullName ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.Email ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.MobileNumber ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.Status ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.PasswordSalt ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.MaskedEmail ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.MaskedMobileNumber ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.EmailHash ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.MobileHash ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(0, NpgsqlTypes.NpgsqlDbType.Integer); // failed_login_attempts
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_failed_login
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // lockout_end
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_login_date
                                writer.Write(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_mfa_enabled
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // mfa_type
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // mfa_secret
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_mfa_sent_at
                                writer.Write(record.ReportingToId ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(0, NpgsqlTypes.NpgsqlDbType.Integer); // lockout_count
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // azureoid
                                writer.Write(record.UserType ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.Currency ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.Location ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text); // client_sap_code
                                writer.Write(record.DigitalSignature ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // last_password_changed
                                writer.Write(true, NpgsqlTypes.NpgsqlDbType.Boolean); // is_active
                                writer.Write(0, NpgsqlTypes.NpgsqlDbType.Integer); // created_by
                                writer.Write(now, NpgsqlTypes.NpgsqlDbType.Timestamp); // created_date
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // modified_by
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // modified_date
                                writer.Write(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_deleted
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // deleted_by
                                writer.Write(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // deleted_date
                                writer.Write(record.ErpUsername ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.ApprovalHead ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(record.Location ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text); // time_zone_country
                                writer.Write(record.DigitalSignaturePath ?? string.Empty, NpgsqlTypes.NpgsqlDbType.Text);
                            }

                            Interlocked.Add(ref insertedCount, batchList.Count);

                            // recycle buffer list back into pool for reuse
                            batchList.Clear();
                            listPool.Add(batchList);
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref errorCount);
                            progress.ReportError($"Error writing record chunk to PostgreSQL: {ex.Message}", processedCount);
                            cts.Cancel();
                            throw;
                        }
                    }

                    await writer.CompleteAsync();
                }
                catch (Exception ex)
                {
                    backgroundException = ex;
                    cts.Cancel();
                }
                finally
                {
                    if (writerConn != null && writerConn != pgConn)
                    {
                        try
                        {
                            writerConn.Close();
                            writerConn.Dispose();
                        }
                        catch { }
                    }
                }
            }, token));
        }

        // Start transformation worker pool
        var transformTasks = new List<Task>();
        for (int w = 0; w < transformWorkerCount; w++)
        {
            transformTasks.Add(Task.Run(() =>
            {
                try
                {
                    var localBatch = listPool.TryTake(out var pooledList) ? pooledList : new List<UserRecord>(TRANSFORM_BATCH_SIZE);
                    foreach (var raw in rawQueue.GetConsumingEnumerable(token))
                    {
                        if (token.IsCancellationRequested) break;
                        try
                        {
                            var userRecord = BuildUserRecordFromRaw(raw);
                            if (userRecord != null)
                            {
                                localBatch.Add(userRecord);
                                if (localBatch.Count >= TRANSFORM_BATCH_SIZE)
                                {
                                    writeQueue.Add(localBatch, token);
                                    localBatch = listPool.TryTake(out pooledList) ? pooledList : new List<UserRecord>(TRANSFORM_BATCH_SIZE);
                                }
                            }
                            else
                            {
                                Interlocked.Increment(ref skippedCount);
                            }
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref errorCount);
                            progress.ReportError($"Error transforming record: {ex.Message}", Interlocked.CompareExchange(ref processedCount, 0, 0));
                        }
                    }

                    if (localBatch.Count > 0)
                    {
                        writeQueue.Add(localBatch, token);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // ignore
                }
                catch (Exception ex)
                {
                    backgroundException = ex;
                    cts.Cancel();
                }
            }, token));
        }

        progress.ReportProgress(0, totalRecords, "Starting migration (parallel transform + single-writer COPY)...", stopwatch.Elapsed);

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        sqlCmd.CommandTimeout = 300;
        using var reader = await sqlCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        try
        {
            // cache ordinals once for performance
            var ordPersonId = reader.GetOrdinal("PERSON_ID");
            var ordUserId = reader.GetOrdinal("USER_ID");
            var ordPassword = reader.GetOrdinal("USERPASSWORD");
            var ordFullName = reader.GetOrdinal("FULL_NAME");
            var ordEmail = reader.GetOrdinal("EMAIL_ADDRESS");
            var ordMobile = reader.GetOrdinal("MobileNumber");
            var ordStatus = reader.GetOrdinal("STATUS");
            var ordReporting = reader.GetOrdinal("REPORTINGTO");
            var ordUserType = reader.GetOrdinal("USERTYPE_ID");
            var ordCurrency = reader.GetOrdinal("CURRENCYID");
            var ordLocation = reader.GetOrdinal("TIMEZONE");
            var ordErp = reader.GetOrdinal("USER_SAP_ID");
            var ordApproval = reader.GetOrdinal("DEPARTMENTHEAD");
            var ordDigital = reader.GetOrdinal("DigitalSignature");

            while (await reader.ReadAsync())
            {
                if (token.IsCancellationRequested) break;

                Interlocked.Increment(ref processedCount);
                try
                {
                    var raw = new RawUserRow
                    {
                        PersonId = reader.IsDBNull(ordPersonId) ? 0 : reader.GetInt32(ordPersonId),
                        UserId = reader.IsDBNull(ordUserId) ? string.Empty : reader.GetString(ordUserId),
                        RawPassword = reader.IsDBNull(ordPassword) ? string.Empty : reader.GetString(ordPassword),
                        FullName = reader.IsDBNull(ordFullName) ? string.Empty : reader.GetString(ordFullName),
                        Email = reader.IsDBNull(ordEmail) ? string.Empty : reader.GetString(ordEmail),
                        MobileNumber = reader.IsDBNull(ordMobile) ? string.Empty : reader.GetString(ordMobile),
                        Status = reader.IsDBNull(ordStatus) ? string.Empty : reader.GetString(ordStatus),
                        ReportingTo = reader.IsDBNull(ordReporting) ? string.Empty : reader.GetString(ordReporting),
                        UserType = reader.IsDBNull(ordUserType) ? string.Empty : reader.GetString(ordUserType),
                        Currency = reader.IsDBNull(ordCurrency) ? string.Empty : reader.GetString(ordCurrency),
                        Location = reader.IsDBNull(ordLocation) ? string.Empty : reader.GetString(ordLocation),
                        ErpUsername = reader.IsDBNull(ordErp) ? string.Empty : reader.GetString(ordErp),
                        ApprovalHead = reader.IsDBNull(ordApproval) ? string.Empty : reader.GetString(ordApproval),
                        DigitalSignature = reader.IsDBNull(ordDigital) ? string.Empty : reader.GetString(ordDigital)
                    };

                    rawQueue.Add(raw, token);

                    if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
                    {
                        progress.ReportProgress(processedCount, totalRecords, $"Queued: {processedCount:N0}, Inserted: {insertedCount:N0}, Skipped: {skippedCount:N0}, Errors: {errorCount:N0}", stopwatch.Elapsed);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref errorCount);
                    progress.ReportError($"Error reading raw record {processedCount}: {ex.Message}", processedCount);
                    // optionally continue
                }
            }

            // Signal no more raw items
            rawQueue.CompleteAdding();

            // Wait for transforms to finish
            await Task.WhenAll(transformTasks);

            // Signal writer no more items
            writeQueue.CompleteAdding();

            // Wait for backgrond writer tasks to finish
            if (writerTasks != null && writerTasks.Count > 0)
            {
                await Task.WhenAll(writerTasks);
            }
            else
            {
                // fallback to previously used single writerTask
                await writerTask;
            }

            if (backgroundException != null)
            {
                throw backgroundException;
            }
        }
        catch (Exception ex)
        {
            cts.Cancel();
            progress.ReportError($"Migration failed after processing {processedCount} records: {ex.Message}", processedCount);
            throw;
        }
        finally
        {
            rawQueue.Dispose();
            writeQueue.Dispose();
            cts.Dispose();
        }

        stopwatch.Stop();
        progress.ReportCompleted(processedCount, insertedCount, stopwatch.Elapsed);
        return insertedCount;
    }

    private int AdjustBatchSize(int currentBatchSize, int recordsInserted)
    {
        if (recordsInserted < currentBatchSize * 0.8)
        {
            return Math.Max(currentBatchSize / 2, 100); // Reduce batch size
        }
        else if (recordsInserted == currentBatchSize)
        {
            return Math.Min(currentBatchSize * 2, 5000); // Increase batch size
        }
        return currentBatchSize; // Keep batch size unchanged
    }

    private UserRecord? ReadUserRecord(SqlDataReader reader, int recordNumber)
    {
        try
        {
            var (passwordHash, passwordSalt) = PasswordEncryptionHelper.EncryptPassword(reader["USERPASSWORD"]?.ToString() ?? string.Empty);
            var emailAddress = reader["EMAIL_ADDRESS"].ToString() ?? string.Empty;
            var mobileNumber = reader["MobileNumber"].ToString() ?? string.Empty;
            var encryptedEmail = _aesEncryptionService.Encrypt(emailAddress);
            var encryptedMobileNumber = _aesEncryptionService.Encrypt(mobileNumber);
            var emailHash = AesEncryptionService.ComputeSha256Hash(emailAddress);
            var mobileHash = AesEncryptionService.ComputeSha256Hash(mobileNumber);
            return new UserRecord
            {
                UserId = Convert.ToInt32(reader["PERSON_ID"]),
                Username = reader["USER_ID"].ToString() ?? "",
                PasswordHash = passwordHash,
                PasswordSalt = passwordSalt,
                FullName = reader["FULL_NAME"].ToString() ?? "",
                Email = encryptedEmail,
                MobileNumber = encryptedMobileNumber,
                Status = reader["STATUS"].ToString() ?? "",
                MaskedEmail = MaskHelper.MaskEmail(emailAddress),
                MaskedMobileNumber = MaskHelper.MaskPhoneNumber(mobileNumber),
                EmailHash = emailHash,
                MobileHash = mobileHash,
                ReportingToId = reader["REPORTINGTO"].ToString() ?? "",
                UserType = reader["USERTYPE_ID"].ToString() ?? "",
                Currency = reader["CURRENCYID"].ToString() ?? "",
                Location = reader["TIMEZONE"].ToString() ?? "",
                ErpUsername = reader["USER_SAP_ID"].ToString() ?? "",
                ApprovalHead = reader["DEPARTMENTHEAD"].ToString() ?? "",
                DigitalSignature = reader["DigitalSignature"].ToString() ?? "",
                DigitalSignaturePath = "/Documents/TechnicalDocuments/" + (reader["DigitalSignature"].ToString() ?? "")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading user record {recordNumber}: {ex.Message}");
            return null;
        }
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<UserRecord> batch, NpgsqlTransaction? transaction = null)
    {
        if (batch.Count == 0) return 0;
        try
        {
            return await InsertBatchWithCopyAsync(pgConn, batch, transaction);
        }
        catch (Exception ex)
        {
            throw new Exception($"Error inserting batch of {batch.Count} records: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchWithCopyAsync(NpgsqlConnection pgConn, List<UserRecord> batch, NpgsqlTransaction? transaction = null)
    {
        var copyCommand = @"COPY users (
            user_id, username, password_hash, full_name, email, mobile_number, status,
            password_salt, masked_email, masked_mobile_number, email_hash, mobile_hash, failed_login_attempts,
            last_failed_login, lockout_end, last_login_date, is_mfa_enabled, mfa_type, mfa_secret, last_mfa_sent_at,
            reporting_to_id, lockout_count, azureoid, user_type, currency, location, client_sap_code,
            digital_signature, last_password_changed, is_active, created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date, erp_username, approval_head, time_zone_country, digital_signature_path
        ) FROM STDIN (FORMAT BINARY)";
        using var writer = await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None);
        var now = DateTime.UtcNow;
        foreach (var record in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(record.UserId);
            await writer.WriteAsync(record.Username);
            await writer.WriteAsync(record.PasswordHash);
            await writer.WriteAsync(record.FullName);
            await writer.WriteAsync(record.Email);
            await writer.WriteAsync(record.MobileNumber);
            await writer.WriteAsync(record.Status);
            await writer.WriteAsync(record.PasswordSalt);
            await writer.WriteAsync(record.MaskedEmail);
            await writer.WriteAsync(record.MaskedMobileNumber);
            await writer.WriteAsync(record.EmailHash);
            await writer.WriteAsync(record.MobileHash);
            await writer.WriteAsync(0); // failed_login_attempts
            await writer.WriteAsync(DBNull.Value); // last_failed_login
            await writer.WriteAsync(DBNull.Value); // lockout_end
            await writer.WriteAsync(DBNull.Value); // last_login_date
            await writer.WriteAsync(false); // is_mfa_enabled
            await writer.WriteAsync(DBNull.Value); // mfa_type
            await writer.WriteAsync(DBNull.Value); // mfa_secret
            await writer.WriteAsync(DBNull.Value); // last_mfa_sent_at
            await writer.WriteAsync(record.ReportingToId);
            await writer.WriteAsync(0); // lockout_count
            await writer.WriteAsync(DBNull.Value); // azureoid
            await writer.WriteAsync(record.UserType);
            await writer.WriteAsync(record.Currency);
            await writer.WriteAsync(record.Location);
            await writer.WriteAsync(DBNull.Value); // client_sap_code
            await writer.WriteAsync(record.DigitalSignature);
            await writer.WriteAsync(DBNull.Value); // last_password_changed
            await writer.WriteAsync(true); // is_active
            await writer.WriteAsync(0); // created_by
            await writer.WriteAsync(now); // created_date
            await writer.WriteAsync(DBNull.Value); // modified_by
            await writer.WriteAsync(DBNull.Value); // modified_date
            await writer.WriteAsync(false); // is_deleted
            await writer.WriteAsync(DBNull.Value); // deleted_by
            await writer.WriteAsync(DBNull.Value); // deleted_date
            await writer.WriteAsync(record.ErpUsername);
            await writer.WriteAsync(record.ApprovalHead);
            await writer.WriteAsync(record.Location); // time_zone_country
            await writer.WriteAsync(record.DigitalSignaturePath);
        }
        await writer.CompleteAsync();
        return batch.Count;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // user_id
            "Direct", // username
            "Direct", // password_hash
            "Direct", // full_name
            "Direct", // email
            "Direct", // mobile_number
            "Direct", // status
            "Default: null", // password_salt
            "Direct", // masked_email
            "Direct", // masked_mobile_number
            "Default: null", // email_hash
            "Default: null", // mobile_hash
            "Default: 0", // failed_login_attempts
            "Default: null", // last_failed_login
            "Default: null", // lockout_end
            "Default: null", // last_login_date
            "Default: false", // is_mfa_enabled
            "Default: null", // mfa_type
            "Default: null", // mfa_secret
            "Default: null", // last_mfa_sent_at
            "Default: null", // reporting_to_id
            "Default: 0", // lockout_count
            "Default: null", // azureoid
            "Direct", // user_type
            "Direct", // currency
            "Direct", // location
            "Default: null", // client_sap_code
            "Direct", // digital_signature
            "Default: null", // last_password_changed
            "Default: true", // is_active
            "Default: 0", // created_by
            "Default: Now", // created_date
            "Default: null", // modified_by
            "Default: null", // modified_date
            "Default: false", // is_deleted
            "Default: null", // deleted_by
            "Default: null", // deleted_date
            "Direct", // erp_username
            "Direct", // approval_head
            "Default: null", // time_zone_country
            "Default: null" // digital_signature_path
        };
    }

    private class UserRecord
    {
        public int UserId { get; set; }
        public string Username { get; set; } = "";
        public string PasswordHash { get; set; } = "";
        public string PasswordSalt { get; set; } = "";
        public string FullName { get; set; } = "";
        public string Email { get; set; } = "";
        public string MobileNumber { get; set; } = "";
        public string Status { get; set; } = "";
        public string MaskedEmail { get; set; } = "";
        public string MaskedMobileNumber { get; set; } = "";
        public string EmailHash { get; set; } = "";
        public string MobileHash { get; set; } = "";
        public string ReportingToId { get; set; } = "";
        public string UserType { get; set; } = "";
        public string Currency { get; set; } = "";
        public string Location { get; set; } = "";
        public string ErpUsername { get; set; } = "";
        public string ApprovalHead { get; set; } = "";
        public string DigitalSignature { get; set; } = "";
        public string DigitalSignaturePath { get; set; } = "";
    }
}