using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace SeedData.Models;

    public class AuditModels
    {
        [Column("created_by")]
        public int? CreatedBy { get; set; }

        [Column("created_date")]
        public DateTime? CreatedDate { get; set; }

        [Column("modified_by")]
        public int? ModifiedBy { get; set; }

        [Column("modified_date")]
        public DateTime? ModifiedDate { get; set; }

        [Column("is_deleted")]
        public bool? IsDeleted { get; set; }

        [Column("deleted_by")]
        public int? DeletedBy { get; set; }

        [Column("deleted_date")]
        public DateTime? DeletedDate { get; set; }
    }
    public static class AuditModelBuilderExtensions
    {
        public static void ConfigureAuditFields<T>(this EntityTypeBuilder<T> builder) where T : AuditModels
        {
            builder.Property(e => e.CreatedBy).HasColumnName("created_by");
            builder.Property(e => e.CreatedDate)
                .HasColumnName("created_date")
                .HasColumnType("timestamp(3) with time zone");
            builder.Property(e => e.ModifiedBy).HasColumnName("modified_by");
            builder.Property(e => e.ModifiedDate)
                .HasColumnName("modified_date")
                .HasColumnType("timestamp(3) with time zone");
            builder.Property(e => e.IsDeleted).HasColumnName("is_deleted");
            builder.Property(e => e.DeletedBy).HasColumnName("deleted_by");
            builder.Property(e => e.DeletedDate)
                .HasColumnName("deleted_date")
                .HasColumnType("timestamp(3) with time zone");
        }
    }
