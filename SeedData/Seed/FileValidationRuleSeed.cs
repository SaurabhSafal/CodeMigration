using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    public static class FileValidationRuleSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FileValidationRule>(entity =>
            {
                entity.HasData(
                    // Permission Group 1 rules
                    new FileValidationRule
                    {
                        RuleId = 1,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".pdf",
                        MaxSizeMB = 5,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 2,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".docx",
                        MaxSizeMB = 10,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 3,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".xlsx",
                        MaxSizeMB = 30,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 4,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".csv",
                        MaxSizeMB = 15,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 5,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".jpg",
                        MaxSizeMB = 10,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 6,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".png",
                        MaxSizeMB = 12,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 7,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".gif",
                        MaxSizeMB = 8,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 8,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".txt",
                        MaxSizeMB = 5,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 9,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".zip",
                        MaxSizeMB = 50,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 10,
                        CompanyId = 1,
                        PermissionGroupId = 1,
                        Extension = ".pptx",
                        MaxSizeMB = 40,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    // Permission Group 2 rules
                    new FileValidationRule
                    {
                        RuleId = 11,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".pdf",
                        MaxSizeMB = 25,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 12,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".docx",
                        MaxSizeMB = 20,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 13,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".xlsx",
                        MaxSizeMB = 30,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 14,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".csv",
                        MaxSizeMB = 15,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 15,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".jpg",
                        MaxSizeMB = 10,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 16,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".png",
                        MaxSizeMB = 12,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 17,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".gif",
                        MaxSizeMB = 8,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 18,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".txt",
                        MaxSizeMB = 5,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 19,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".zip",
                        MaxSizeMB = 50,
                        CreatedBy = 1,

                        IsDeleted = false
                    },
                    new FileValidationRule
                    {
                        RuleId = 20,
                        CompanyId = 1,
                        PermissionGroupId = 2,
                        Extension = ".pptx",
                        MaxSizeMB = 40,
                        CreatedBy = 1,

                        IsDeleted = false
                    }
                );
            });
        }
    }
}
