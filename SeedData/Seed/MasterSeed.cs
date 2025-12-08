using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    /// <summary>
    /// Master seed class that orchestrates all entity seeding in the correct order
    /// </summary>
    public static class MasterSeed
    {
        /// <summary>
        /// Seeds all entities in the correct dependency order
        /// </summary>
        /// <param name="modelBuilder">The model builder</param>
        public static void SeedAllData(ModelBuilder modelBuilder)
        {
            // Seed master data first (no dependencies)
            CompanyMasterSeed.SeedData(modelBuilder);
            CountryMasterSeed.SeedData(modelBuilder);
            UserAuditActionSeed.SeedData(modelBuilder);
            FileValidationRuleSeed.SeedData(modelBuilder);

            // Seed permission-related data (depends on permission groups)
            PermissionGroupSeed.SeedData(modelBuilder);
            PermissionSeed.SeedData(modelBuilder);

            // Seed roles
            RoleSeed.SeedData(modelBuilder);

            // Seed permission templates (depends on roles, permission groups, and permissions)
            PermissionsTemplateSeed.SeedData(modelBuilder);
        }
    }
}
