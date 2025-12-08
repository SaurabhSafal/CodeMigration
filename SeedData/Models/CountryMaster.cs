using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SeedData.Models;

[Table("country_master")]
public class CountryMaster : AuditModels
{
    [Key]
    [Column("countryid")]
    public int CountryID { get; set; }

    [Column("countryname")]
    public string CountryName { get; set; }

    [Column("countrycode")]
    public string CountryCode { get; set; }
}