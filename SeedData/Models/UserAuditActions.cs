using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SeedData.Models
{
    [Table("user_audit_actions")]
    public class UserAuditAction : AuditModels
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Column("action_name_id")]
        public int Id { get; set; }

        [Column("action_name")]
        public string? ActionName { get; set; }

        [Column("action_description")]
        public string? ActionDescription { get; set; }

        [Column("action_type")]
        public string? ActionType { get; set; }
    }
}
