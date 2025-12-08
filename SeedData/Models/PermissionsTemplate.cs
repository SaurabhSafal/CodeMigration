using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SeedData.Models
{
    /// <summary>
    /// Maps which permission group contains which permissions.
    /// </summary>
    [Table("permissions_template")]
    public class PermissionsTemplate : AuditModels
    {
        [Key]
        [Column("id")]
        public int Id { get; set; }

        [Column("role_id")]
        public int RoleId { get; set; }

        [Column("permission_group_id")]
        public int PermissionGroupId { get; set; }

        [Column("permission_id")]
        public int PermissionId { get; set; }

        [ForeignKey("PermissionGroupId")]
        public virtual PermissionGroup PermissionGroup { get; set; }

        [ForeignKey("PermissionId")]
        public virtual Permission Permission { get; set; }

        [ForeignKey("RoleId")]
        public virtual Role Role { get; set; }
    }
}
