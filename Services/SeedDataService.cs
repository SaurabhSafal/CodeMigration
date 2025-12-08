using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataMigration.Services
{
    public class SeedDataService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<SeedDataService> _logger;

        public SeedDataService(IConfiguration configuration, ILogger<SeedDataService> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task<(bool Success, string Message, int RecordsInserted, List<string> TablesSeeded)> RunSeedDataAsync()
        {
            try
            {
                _logger.LogInformation("Starting seed data migration...");

                var pgConnString = _configuration.GetConnectionString("PostgreSql");
                if (string.IsNullOrEmpty(pgConnString))
                {
                    _logger.LogError("PostgreSQL connection string not found in configuration");
                    return (false, "PostgreSQL connection string not found in configuration", 0, new List<string>());
                }

                using var pgConn = new NpgsqlConnection(pgConnString);
                await pgConn.OpenAsync();

                int totalRecordsInserted = 0;
                var seedTables = new List<string>();

                // Seed tables in the correct order (respecting foreign key dependencies)
                totalRecordsInserted += await SeedRolesAsync(pgConn, seedTables);
                totalRecordsInserted += await SeedPermissionGroupsAsync(pgConn, seedTables);
                totalRecordsInserted += await SeedPermissionsAsync(pgConn, seedTables);
                totalRecordsInserted += await SeedPermissionsTemplateAsync(pgConn, seedTables);
                totalRecordsInserted += await SeedUserAuditActionAsync(pgConn, seedTables);

                await pgConn.CloseAsync();

                var message = $"Seed data migration completed. {totalRecordsInserted} records inserted into {seedTables.Count} table(s): {string.Join(", ", seedTables)}";
                _logger.LogInformation(message);

                return (true, message, totalRecordsInserted, seedTables);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred during seed data migration.");
                return (false, ex.Message, 0, new List<string>());
            }
        }

        private async Task<int> SeedRolesAsync(NpgsqlConnection pgConn, List<string> seedTables)
        {
            // Create table if it doesn't exist
            var createTableQuery = @"
                CREATE TABLE IF NOT EXISTS roles (
                    role_id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    description VARCHAR(200),
                    created_by INTEGER,
                    created_date TIMESTAMP(3) WITH TIME ZONE,
                    modified_by INTEGER,
                    modified_date TIMESTAMP(3) WITH TIME ZONE,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    deleted_by INTEGER,
                    deleted_date TIMESTAMP(3) WITH TIME ZONE
                )";
            
            using var createCmd = new NpgsqlCommand(createTableQuery, pgConn);
            await createCmd.ExecuteNonQueryAsync();
            _logger.LogInformation("Ensured roles table exists");

            var checkQuery = "SELECT COUNT(*) FROM roles";
            using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());

            if (count > 0)
            {
                _logger.LogInformation($"roles table already has {count} records, skipping seed");
                return 0;
            }

            _logger.LogInformation("Seeding roles table...");
            int recordsInserted = 0;

            var roles = new[]
            {
                (1, "Super Admin", "Has full access to all features and modules"),
                (2, "Admin", "Has administrative access to most features"),
                (3, "Buyer", "Can create and manage purchase requisitions and events"),
                (4, "Approver", "Can approve purchase requisitions and awards"),
                (5, "Viewer", "Can view information but cannot make changes")
            };

            foreach (var role in roles)
            {
                var insertQuery = @"
                    INSERT INTO roles (role_id, name, description, created_date, is_deleted)
                    VALUES (@roleId, @name, @description, @createdDate, false)
                    ON CONFLICT (role_id) DO NOTHING";

                using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
                insertCmd.Parameters.AddWithValue("roleId", role.Item1);
                insertCmd.Parameters.AddWithValue("name", role.Item2);
                insertCmd.Parameters.AddWithValue("description", role.Item3);
                insertCmd.Parameters.AddWithValue("createdDate", DateTime.UtcNow);

                var rowsAffected = await insertCmd.ExecuteNonQueryAsync();
                recordsInserted += rowsAffected;
            }

            seedTables.Add("roles");
            _logger.LogInformation($"Seeded {recordsInserted} records into roles table");

            return recordsInserted;
        }

        private async Task<int> SeedPermissionGroupsAsync(NpgsqlConnection pgConn, List<string> seedTables)
        {
            // Create table if it doesn't exist
            var createTableQuery = @"
                CREATE TABLE IF NOT EXISTS permission_group (
                    permission_group_id SERIAL PRIMARY KEY,
                    permission_group_name VARCHAR(100) NOT NULL UNIQUE,
                    display_name VARCHAR(150) NOT NULL,
                    icon VARCHAR(100),
                    order_index INTEGER,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_by INTEGER,
                    created_date TIMESTAMP(3) WITH TIME ZONE,
                    modified_by INTEGER,
                    modified_date TIMESTAMP(3) WITH TIME ZONE,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    deleted_by INTEGER,
                    deleted_date TIMESTAMP(3) WITH TIME ZONE
                )";
            
            using var createCmd = new NpgsqlCommand(createTableQuery, pgConn);
            await createCmd.ExecuteNonQueryAsync();
            _logger.LogInformation("Ensured permission_group table exists");

            var checkQuery = "SELECT COUNT(*) FROM permission_group";
            using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());

            if (count > 0)
            {
                _logger.LogInformation($"permission_group table already has {count} records, skipping seed");
                return 0;
            }

            _logger.LogInformation("Seeding permission_group table...");
            int recordsInserted = 0;

            var permissionGroups = new[]
            {
                (1, "PR", "Purchase Requisition", "file-text", 1),
                (2, "Event", "Event Management", "calendar", 2),
                (3, "NFA", "Negotiation For Award", "award", 3),
                (4, "StandaloneNFA", "Standalone NFA", "file-plus", 4),
                (5, "AwardList", "Award List", "list", 5),
                (6, "Supplier", "Supplier Management", "users", 6),
                (7, "PO", "Purchase Order", "shopping-cart", 7),
                (8, "ARC", "Approval Request for Contract", "file-check", 8)
            };

            foreach (var group in permissionGroups)
            {
                var insertQuery = @"
                    INSERT INTO permission_group (permission_group_id, permission_group_name, display_name, icon, order_index, is_active, created_date, is_deleted)
                    VALUES (@id, @name, @displayName, @icon, @orderIndex, true, @createdDate, false)
                    ON CONFLICT (permission_group_id) DO NOTHING";

                using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
                insertCmd.Parameters.AddWithValue("id", group.Item1);
                insertCmd.Parameters.AddWithValue("name", group.Item2);
                insertCmd.Parameters.AddWithValue("displayName", group.Item3);
                insertCmd.Parameters.AddWithValue("icon", group.Item4);
                insertCmd.Parameters.AddWithValue("orderIndex", group.Item5);
                insertCmd.Parameters.AddWithValue("createdDate", DateTime.UtcNow);

                var rowsAffected = await insertCmd.ExecuteNonQueryAsync();
                recordsInserted += rowsAffected;
            }

            seedTables.Add("permission_group");
            _logger.LogInformation($"Seeded {recordsInserted} records into permission_group table");

            return recordsInserted;
        }

        private async Task<int> SeedPermissionsAsync(NpgsqlConnection pgConn, List<string> seedTables)
        {
            // Create table if it doesn't exist
            var createTableQuery = @"
                CREATE TABLE IF NOT EXISTS permissions (
                    permission_id SERIAL PRIMARY KEY,
                    permission_name VARCHAR(100) NOT NULL UNIQUE,
                    description VARCHAR(255),
                    permission_group_id INTEGER NOT NULL,
                    created_by INTEGER,
                    created_date TIMESTAMP(3) WITH TIME ZONE,
                    modified_by INTEGER,
                    modified_date TIMESTAMP(3) WITH TIME ZONE,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    deleted_by INTEGER,
                    deleted_date TIMESTAMP(3) WITH TIME ZONE,
                    FOREIGN KEY (permission_group_id) REFERENCES permission_group(permission_group_id)
                )";
            
            using var createCmd = new NpgsqlCommand(createTableQuery, pgConn);
            await createCmd.ExecuteNonQueryAsync();
            _logger.LogInformation("Ensured permissions table exists");

            var checkQuery = "SELECT COUNT(*) FROM permissions";
            using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());

            if (count > 0)
            {
                _logger.LogInformation($"permissions table already has {count} records, skipping seed");
                return 0;
            }

            _logger.LogInformation("Seeding permissions table...");
            int recordsInserted = 0;

            // PR Permissions (Group 1)
            var permissions = new List<(int id, string name, string description, int groupId)>
            {
                // PR Module Permissions (Group 1)
                (1, "PR.Delegation.Full", "Full PR delegation access", 1),
                (2, "PR.Delegation.Restricted", "Restricted PR delegation access", 1),
                (3, "PR.View.All", "View all PRs", 1),
                (4, "PR.View.Restricted", "View PRs with restrictions", 1),
                (5, "PR.Create.Temporary", "Create temporary PR", 1),
                (6, "PR.Delete.Temporary.Full", "Full deletion of temporary PR", 1),
                (7, "PR.Delete.Temporary.Restricted", "Restricted deletion of temporary PR", 1),
                (8, "PR.UploadDownload.BulkTemporary", "Bulk upload/download temporary PR", 1),
                (9, "PR.Fetch.FromERP", "Fetch PR from ERP", 1),
                (10, "PR.Create.RFQ", "Create RFQ PR", 1),
                (11, "PR.Create.RepeatPO", "Create repeat PO PR", 1),
                (12, "PR.Create.ARCPO", "Create ARC PO PR", 1),
                (13, "PR.Create.Auction", "Create auction PR", 1),

                // Event Create Permissions (Group 2)
                (14, "Event.Create.PRRFQ", "Create RFQ event from PR", 2),
                (15, "Event.Cretae.PRAuction", "Create auction event from PR", 2),
                (16, "Event.Create.StandaloneRFQ", "Create standalone RFQ event", 2),
                (17, "Event.Create.StandaloneAuction", "Create standalone auction event", 2),
                (18, "Event.Create.UploadDownloadTemplate.RFQ", "Upload/download RFQ template", 2),
                (19, "Event.Create.UploadDownloadTemplate.Auction", "Upload/download auction template", 2),

                // Event General Permissions (Group 2)
                (20, "Event.View.All", "View all events", 2),
                (21, "Event.View.Restricted", "View events with restrictions", 2),
                (22, "Event.Create.button", "Show create event button", 2),
                (23, "Event.Delete.Restricted", "Delete own events", 2),
                (24, "Event.Delete.Full", "Delete any event", 2),
                (25, "Event.Terminate.Restricted", "Terminate own events", 2),
                (26, "Event.Terminate.Full", "Terminate any event", 2),
                (27, "Event.RecallPartialQty", "Recall partial quantity", 2),
                (28, "Event.Copy", "Copy event", 2),

                // Technical Docs (Group 2)
                (29, "Event.Upload.TechnicalDocument", "Upload technical document", 2),
                (30, "Event.UploadVendorSpecific.TechnicalDocument", "Upload vendor-specific technical document", 2),
                (31, "Event.Delete.TechnicalDocument", "Delete technical document", 2),

                // Technical Parameters (Group 2)
                (32, "Event.Add.TechnicalParameters", "Add technical parameters", 2),
                (33, "Event.Delete.TechnicalParameters", "Delete technical parameters", 2),
                (34, "Event.ImportTemplate.TechnicalParameters.Full", "Import technical parameters template (full)", 2),
                (35, "Event.ImportTemplate.TechnicalParameters.Restricted", "Import technical parameters template (restricted)", 2),
                (36, "Event.UploadDownload.TechnicalParameters", "Upload/download technical parameters", 2),

                // Technical Approval Workflow (Group 2)
                (37, "Event.Add.TechnicalApproval", "Add technical approval workflow", 2),
                (38, "Event.Delete.TechnicalApproval", "Delete technical approval workflow", 2),
                (39, "Event.Recall.TechnicalApproval", "Recall technical approval workflow", 2),

                // Terms & Conditions (Group 2)
                (40, "Event.Add.TermsandCondition", "Add terms and conditions", 2),
                (41, "Event.Delete.TermsandCondition", "Delete terms and conditions", 2),
                (42, "Event.ImportTemplate.TermsandCondition", "Import T&C template", 2),
                (43, "Event.UploadDownload.TermsandCondition", "Upload/download T&C", 2),

                // Supplier (Group 2)
                (44, "Event.Add.Supplier", "Add supplier to event", 2),
                (45, "Event.Delete.Supplier", "Delete supplier from event", 2),
                (46, "Event.AddafterPublished.Supplier", "Add supplier after event published", 2),

                // Schedule (Group 2)
                (47, "Event.Save.Schedule", "Save event schedule", 2),
                (48, "Event.SaveafterPublished.Schedule", "Change schedule after published", 2),

                // Collaboration (Group 2)
                (49, "Event.Add.Collaboration", "Add collaboration user", 2),
                (50, "Event.Delete.Collaboration", "Delete collaboration user", 2),
                (51, "Event.TransferUser.Collaboration", "Transfer buyer collaboration", 2),

                // Items & Price Bid (Group 2)
                (52, "Event.AddItem.Pricebid", "Add item to price bid", 2),
                (53, "Event.DeleteItem.Pricebid", "Delete item from price bid", 2),
                (54, "Event.ChangeQty.Pricebid", "Change quantity in price bid", 2),
                (55, "Event.AddExtraColumns.Pricebid", "Add extra columns/remarks", 2),
                (56, "Event.Save.Pricebid", "Save price bid", 2),

                // Settings & Comparison (Group 2)
                (57, "Event.ChangeSetting", "Change event settings", 2),
                (58, "Event.PricebidComparision", "View price bid comparison", 2),
                (59, "Event.BidOptimization", "Perform bid optimization", 2),
                (60, "Event.SurrogateBidding", "Surrogate bidding", 2),
                (61, "Event.DownloadComparision", "Download comparison", 2),

                // Publish (Group 2)
                (62, "Event.Published", "Publish event", 2),

                // Next Round (Group 2)
                (63, "Event.NextRound.RFQ", "Create next round for RFQ", 2),
                (64, "Event.NextRound.Auction", "Create next round for auction", 2),

                // NFA Permissions (Group 3)
                (65, "Event.Create.NFA", "Create NFA", 3),
                (66, "Event.Recall.NFA", "Recall NFA", 3),
                (67, "Event.Delete.NFA", "Delete NFA", 3),
                (68, "NFA.Clarification", "NFA clarification", 3),
                (69, "NFA.Hold", "Hold NFA", 3),
                (70, "NFA.CreatePO", "Create PO from NFA", 3),
                (71, "NFA.UpdatePONumber", "Update PO number", 3),
                (72, "NFA.Delete.PO", "Delete PO from NFA", 3),

                // Standalone NFA Permissions (Group 4)
                (73, "NFA.Create.Standalone", "Create standalone NFA", 4),
                (74, "NFA.Delete.Standalone", "Delete standalone NFA", 4),
                (75, "NFA.Recall.Standalone", "Recall standalone NFA", 4),

                // Award List Permissions (Group 5)
                (76, "NFA.UnderApprovalView.All", "View all NFAs under approval", 5),
                (77, "NFA.UnderApprovalView.Restricted", "View NFAs under approval (restricted)", 5),
                (78, "NFA.POPendingView.All", "View all PO pending awards", 5),
                (79, "NFA.POPendingView.Restricted", "View PO pending awards (restricted)", 5),
                (80, "NFA.POCreatedView.All", "View all PO created awards", 5),
                (81, "NFA.POCreatedView.Restricted", "View PO created awards (restricted)", 5),
                (82, "NFA.StandaloneView.All", "View all standalone awards", 5),
                (83, "NFA.StandaloneView.Restricted", "View standalone awards (restricted)", 5),
                (84, "NFA.TerminatedView.All", "View all terminated awards", 5),
                (85, "NFA.TerminatedView.Restricted", "View terminated awards (restricted)", 5),

                // Supplier Permissions (Group 6)
                (86, "Supplier.AddTemporary", "Add temporary supplier", 6),
                (87, "Supplier.ConverttoRegular", "Convert temporary to regular supplier", 6),
                (88, "Supplier.Delete", "Delete supplier", 6),

                // PO Permissions (Group 7)
                (89, "PO.View.All", "View all purchase orders", 7),
                (90, "PO.View.Restricted", "View purchase orders (restricted)", 7),
                (91, "PO.Fetch", "Fetch purchase orders", 7),

                // ARC Permissions (Group 8)
                (92, "ARC.Create", "Create ARC", 8),
                (93, "ARC.View.All", "View all ARCs", 8),
                (94, "ARC.View.Restricted", "View ARCs (restricted)", 8),
                (95, "ARC.Delete", "Delete ARC", 8),
                (96, "ARC.Recall", "Recall ARC", 8),
                (97, "ARC.Terminate", "Terminate ARC", 8),
                (98, "ARC.Amendement", "ARC amendment", 8)
            };

            foreach (var permission in permissions)
            {
                var insertQuery = @"
                    INSERT INTO permissions (permission_id, permission_name, description, permission_group_id, created_date, is_deleted)
                    VALUES (@id, @name, @description, @groupId, @createdDate, false)
                    ON CONFLICT (permission_id) DO NOTHING";

                using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
                insertCmd.Parameters.AddWithValue("id", permission.id);
                insertCmd.Parameters.AddWithValue("name", permission.name);
                insertCmd.Parameters.AddWithValue("description", permission.description);
                insertCmd.Parameters.AddWithValue("groupId", permission.groupId);
                insertCmd.Parameters.AddWithValue("createdDate", DateTime.UtcNow);

                var rowsAffected = await insertCmd.ExecuteNonQueryAsync();
                recordsInserted += rowsAffected;
            }

            seedTables.Add("permissions");
            _logger.LogInformation($"Seeded {recordsInserted} records into permissions table");

            return recordsInserted;
        }

        private async Task<int> SeedPermissionsTemplateAsync(NpgsqlConnection pgConn, List<string> seedTables)
        {
            // Create table if it doesn't exist
            var createTableQuery = @"
                CREATE TABLE IF NOT EXISTS permissions_template (
                    id SERIAL PRIMARY KEY,
                    role_id INTEGER NOT NULL,
                    permission_group_id INTEGER NOT NULL,
                    permission_id INTEGER NOT NULL,
                    created_by INTEGER,
                    created_date TIMESTAMP(3) WITH TIME ZONE,
                    modified_by INTEGER,
                    modified_date TIMESTAMP(3) WITH TIME ZONE,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    deleted_by INTEGER,
                    deleted_date TIMESTAMP(3) WITH TIME ZONE,
                    FOREIGN KEY (role_id) REFERENCES roles(role_id),
                    FOREIGN KEY (permission_group_id) REFERENCES permission_group(permission_group_id),
                    FOREIGN KEY (permission_id) REFERENCES permissions(permission_id)
                )";
            
            using var createCmd = new NpgsqlCommand(createTableQuery, pgConn);
            await createCmd.ExecuteNonQueryAsync();
            _logger.LogInformation("Ensured permissions_template table exists");

            var checkQuery = "SELECT COUNT(*) FROM permissions_template";
            using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());

            if (count > 0)
            {
                _logger.LogInformation($"permissions_template table already has {count} records, skipping seed");
                return 0;
            }

            _logger.LogInformation("Seeding permissions_template table...");
            int recordsInserted = 0;

            // Super Admin (Role 1) gets all permissions
            var getPermissionsQuery = "SELECT permission_id, permission_group_id FROM permissions WHERE is_deleted = false OR is_deleted IS NULL";
            using var getCmd = new NpgsqlCommand(getPermissionsQuery, pgConn);
            using var reader = await getCmd.ExecuteReaderAsync();
            
            var permissionsList = new List<(int permissionId, int groupId)>();
            while (await reader.ReadAsync())
            {
                permissionsList.Add((reader.GetInt32(0), reader.GetInt32(1)));
            }
            await reader.CloseAsync();

            int templateId = 1;
            foreach (var perm in permissionsList)
            {
                var insertQuery = @"
                    INSERT INTO permissions_template (id, role_id, permission_group_id, permission_id, created_date, is_deleted)
                    VALUES (@id, @roleId, @groupId, @permissionId, @createdDate, false)
                    ON CONFLICT (id) DO NOTHING";

                using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
                insertCmd.Parameters.AddWithValue("id", templateId);
                insertCmd.Parameters.AddWithValue("roleId", 1); // Super Admin role
                insertCmd.Parameters.AddWithValue("groupId", perm.groupId);
                insertCmd.Parameters.AddWithValue("permissionId", perm.permissionId);
                insertCmd.Parameters.AddWithValue("createdDate", DateTime.UtcNow);

                var rowsAffected = await insertCmd.ExecuteNonQueryAsync();
                recordsInserted += rowsAffected;
                templateId++;
            }

            seedTables.Add("permissions_template");
            _logger.LogInformation($"Seeded {recordsInserted} records into permissions_template table");

            return recordsInserted;
        }

        private async Task<int> SeedUserAuditActionAsync(NpgsqlConnection pgConn, List<string> seedTables)
        {
            // Create table if it doesn't exist
            var createTableQuery = @"
                CREATE TABLE IF NOT EXISTS user_audit_action (
                    id INTEGER PRIMARY KEY,
                    action_name VARCHAR(255) NOT NULL,
                    action_description TEXT,
                    action_type VARCHAR(100)
                )";
            
            using var createCmd = new NpgsqlCommand(createTableQuery, pgConn);
            await createCmd.ExecuteNonQueryAsync();
            _logger.LogInformation("Ensured user_audit_action table exists");

            var checkQuery = "SELECT COUNT(*) FROM user_audit_action";
            using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());

            if (count > 0)
            {
                _logger.LogInformation($"user_audit_action table already has {count} records, skipping seed");
                return 0;
            }

            _logger.LogInformation("Seeding user_audit_action table...");
            int recordsInserted = 0;

            var userAuditActions = new[]
            {
                // Alert Actions
                (1, "PR Delegate", "PR Delegate action", "Alert"),
                (2, "Auto Assigned PR", "Auto Assigned PR action", "Alert"),
                (3, "Add Collaborative User", "Add Collaborative User action", "Alert"),
                (4, "Delete Collaborative User", "Delete Collaborative User action", "Alert"),
                (5, "Transfer Collaborative User", "Transfer Collaborative User action", "Alert"),
                (6, "Assign Technical Approval", "Assign Technical Approval action", "Alert"),
                (7, "Send for Approval NFA for Approver", "Send for Approval NFA for Approver action", "Alert"),
                (8, "Hold NFA", "Hold NFA action", "Alert"),
                (9, "Reject NFA", "Reject NFA action", "Alert"),
                (10, "Approve NFA", "Approve NFA action", "Alert"),
                (11, "All Level Approved NFA", "All Level Approved NFA action", "Alert"),
                (12, "Send for Approval Standalone NFA", "Send for Approval Standalone NFA action", "Alert"),
                (13, "After Publish Event Settings change", "After Publish Event Settings change action", "Alert"),
                (14, "Event Communication", "Event Communication action", "Alert"),
                (15, "Supplier Deviating T&C", "Supplier deviating T&C action", "Alert"),
                (16, "Responding to Deviating T&C", "Responding to deviating T&C action", "Alert"),
                (17, "Send for Approval ARC", "Send for Approval ARC action", "Alert"),
                (18, "Reject ARC", "Reject ARC action", "Alert"),
                (19, "Approve ARC", "Approve ARC action", "Alert"),
                (20, "All Level Approved ARC", "All Level Approved ARC action", "Alert"),
                (46, "NFA Clarification", "NFA Clarification action", "Alert"),
                (48, "Terminate ARC", "Terminate ARC action", "Alert"),
                
                // Notification Actions
                (21, "Create Event", "Create Event action", "Notification"),
                (22, "Terminate Event", "Terminate Event action", "Notification"),
                (23, "Recall-Partial Qty", "Recall-Partial Qty action", "Notification"),
                (24, "After Publish add and Delete supplier", "After Publish add and Delete supplier action", "Notification"),
                (25, "After Publish Change Schedule", "After Publish Change Schedule action", "Notification"),
                (26, "Recall Technical Approval", "Recall Technical Approval action", "Notification"),
                (27, "Publish Event", "Publish Event action", "Notification"),
                (28, "Next Round", "Next Round action", "Notification"),
                (29, "Bid Optimization", "Bid Optimization action", "Notification"),
                (30, "Send for Approval NFA for Reporting Manager", "Send for Approval NFA for Reporting Manager action", "Notification"),
                (31, "Recall NFA", "Recall NFA action", "Notification"),
                (32, "Update PO Number", "Update PO Number action", "Notification"),
                (33, "Send for Approval Standalone NFA", "Send for Approval Standalone NFA action", "Notification"),
                (34, "Create PO", "Create PO action", "Notification"),
                (35, "After Publish Upload Technical Doc by Collaborative User", "After Publish Upload Technical Doc by Collaborative User action", "Notification"),
                (36, "Supplier Participate in Event", "Supplier Participate in Event action", "Notification"),
                (37, "Supplier Regret in Event", "Supplier Regret in Event action", "Notification"),
                (38, "Supplier Accepting T&C", "Supplier deviating T&C action", "Notification"),
                (39, "Supplier Upload Doc", "Supplier Upload Doc action", "Notification"),
                (40, "Supplier Submit Bid", "Supplier Submit Bid action", "Notification"),
                (41, "Buyer Responding to Deviating T&C", "Buyer Responding to Deviating T&C action", "Notification"),
                (42, "Send for Approval ARC", "Send for Approval ARC action", "Notification"),
                (43, "Recall ARC", "Recall ARC action", "Notification"),
                (44, "Approve ARC", "Approve ARC action", "Notification"),
                (45, "Convert to Regular Vendor", "Convert Temp to Regular Vendor action", "Notification"),
                (47, "Terminate NFA", "Terminate NFA action", "Notification"),
            };

            foreach (var action in userAuditActions)
            {
                var insertQuery = @"
                    INSERT INTO user_audit_action (id, action_name, action_description, action_type)
                    VALUES (@id, @actionName, @actionDescription, @actionType)
                    ON CONFLICT (id) DO NOTHING";

                using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
                insertCmd.Parameters.AddWithValue("id", action.Item1);
                insertCmd.Parameters.AddWithValue("actionName", action.Item2);
                insertCmd.Parameters.AddWithValue("actionDescription", action.Item3);
                insertCmd.Parameters.AddWithValue("actionType", (object?)action.Item4 ?? DBNull.Value);

                await insertCmd.ExecuteNonQueryAsync();
                recordsInserted++;
            }

            // Insert additional actions with null ActionType
            var nullTypeActions = new[]
            {
                (49, "NFA Deleted", "NFA Deleted action"),
                (50, "Update Deviation-Term", "Update Deviation-Term Remarks action"),
                (51, "Event Deleted", "Event Deleted action")
            };

            foreach (var action in nullTypeActions)
            {
                var insertQuery = @"
                    INSERT INTO user_audit_action (id, action_name, action_description, action_type)
                    VALUES (@id, @actionName, @actionDescription, NULL)
                    ON CONFLICT (id) DO NOTHING";

                using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
                insertCmd.Parameters.AddWithValue("id", action.Item1);
                insertCmd.Parameters.AddWithValue("actionName", action.Item2);
                insertCmd.Parameters.AddWithValue("actionDescription", action.Item3);

                await insertCmd.ExecuteNonQueryAsync();
                recordsInserted++;
            }

            seedTables.Add("user_audit_action");
            _logger.LogInformation($"Seeded {recordsInserted} records into user_audit_action table");

            return recordsInserted;
        }
    }
}
