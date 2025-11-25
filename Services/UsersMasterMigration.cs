using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Helper;

public class UsersMasterMigration : MigrationService
{
    private readonly AesEncryptionService _aesEncryptionService;

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
        _aesEncryptionService = new AesEncryptionService();
    }

    protected override List<string> GetLogics()
    {
        return new List<string> {
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

    public async Task<int> MigrateAsync()
    {
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();
        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);

        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            var (passwordHash, passwordSalt) = DataMigration.Helper.PasswordEncryptionHelper.EncryptPassword(reader["USERPASSWORD"].ToString());
            
            // Encrypt email and mobile number
            var emailAddress = reader["EMAIL_ADDRESS"].ToString() ?? string.Empty;
            var mobileNumber = reader["MobileNumber"].ToString() ?? string.Empty;
            var encryptedEmail = _aesEncryptionService.Encrypt(emailAddress);
            var encryptedMobileNumber = _aesEncryptionService.Encrypt(mobileNumber);
            var emailHash = AesEncryptionService.ComputeSha256Hash(emailAddress);
            var mobileHash = AesEncryptionService.ComputeSha256Hash(mobileNumber);
            
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@user_id", reader["PERSON_ID"]);
            pgCmd.Parameters.AddWithValue("@username", reader["USER_ID"]);
            pgCmd.Parameters.AddWithValue("@password_hash", passwordHash);
            pgCmd.Parameters.AddWithValue("@full_name", reader["FULL_NAME"]);
            pgCmd.Parameters.AddWithValue("@email", encryptedEmail);
            pgCmd.Parameters.AddWithValue("@mobile_number", encryptedMobileNumber);
            pgCmd.Parameters.AddWithValue("@status", reader["STATUS"]);

            pgCmd.Parameters.AddWithValue("@password_salt", passwordSalt);
            pgCmd.Parameters.AddWithValue("@masked_email", encryptedEmail);
            pgCmd.Parameters.AddWithValue("@masked_mobile_number", encryptedMobileNumber);
            pgCmd.Parameters.AddWithValue("@email_hash", emailHash);
            pgCmd.Parameters.AddWithValue("@mobile_hash", mobileHash);
            pgCmd.Parameters.AddWithValue("@failed_login_attempts", 0);
            pgCmd.Parameters.AddWithValue("@last_failed_login", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lockout_end", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@last_login_date", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_mfa_enabled", false);
            pgCmd.Parameters.AddWithValue("@mfa_type", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@mfa_secret", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@last_mfa_sent_at", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@reporting_to_id", reader["REPORTINGTO"]);
            pgCmd.Parameters.AddWithValue("@lockout_count", 0);
            pgCmd.Parameters.AddWithValue("@azureoid", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@user_type", reader["USERTYPE_ID"]);
            pgCmd.Parameters.AddWithValue("@currency", reader["CURRENCYID"]);
            pgCmd.Parameters.AddWithValue("@location", reader["TIMEZONE"]);
            pgCmd.Parameters.AddWithValue("@client_sap_code", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@digital_signature", reader["DigitalSignature"]);
            pgCmd.Parameters.AddWithValue("@last_password_changed", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_active", true);
            pgCmd.Parameters.AddWithValue("@created_by", 0);
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_deleted", false);
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@erp_username", reader["USER_SAP_ID"]);
            pgCmd.Parameters.AddWithValue("@approval_head", reader["DEPARTMENTHEAD"]);
            pgCmd.Parameters.AddWithValue("@time_zone_country", reader["TIMEZONE"]);
            pgCmd.Parameters.AddWithValue("@digital_signature_path", "/Documents/TechnicalDocuments/" + reader["DigitalSignature"]);

            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}