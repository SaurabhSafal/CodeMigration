using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;


namespace SeedData.Models;

/// <summary>
/// File validation rules for multi-tenant file upload validation
/// </summary>
[Table("file_validation_rules")]
[Index(nameof(CompanyId), nameof(PermissionGroupId), Name = "idx_file_validation_rules_company_permission_group")]
[Index(nameof(CompanyId), Name = "idx_file_validation_rules_company")]
[Index(nameof(PermissionGroupId), Name = "idx_file_validation_rules_permission_group")]
[Index(nameof(IsDeleted), Name = "idx_file_validation_rules_is_deleted")]
public partial class FileValidationRule : AuditModels
{
    /// <summary>
    /// Primary key for the file validation rule
    /// </summary>
    [Key]
    [Column("rule_id")]
    public int RuleId { get; set; }

    /// <summary>
    /// Company ID to which this rule applies   
    /// </summary>
    [Required]
    [Column("company_id")]
    public int CompanyId { get; set; }

    /// <summary>
    /// Permission group ID to which this rule applies
    /// </summary>
    [Required]
    [Column("permission_group_id")]
    public int PermissionGroupId { get; set; }

    /// <summary>
    /// File extension (e.g., .pdf, .csv, .xlsx)
    /// </summary>
    [Required]
    [Column("extension")]
    [StringLength(10)]
    public string Extension { get; set; } = string.Empty;

    /// <summary>
    /// Maximum file size in MB
    /// </summary>
    [Required]
    [Column("max_size_mb")]
    public int MaxSizeMB { get; set; }

    /// <summary>
    /// Navigation property to Company
    /// </summary>
    [ForeignKey(nameof(CompanyId))]
    [InverseProperty("FileValidationRules")]
    public virtual CompanyMaster Company { get; set; } = null!;
}
