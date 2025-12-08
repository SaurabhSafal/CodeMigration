using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    public static class CompanyMasterSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CompanyMaster>(entity =>
            {
                entity.HasData(
                    new CompanyMaster
                    {
                        CompanyId = 1,
                        CompanyCode = "WCLQA",
                        CompanyName = "Welspun Corp Limited",
                        SapVersion = "sap_version",
                        PrAllocationLogic = "Material Group",
                        Address = "Welspun City, Village Versamedi, Taluka Anjar, Dis",
                        CompanyLogoUrl = "WCL-Logo_88c47191-cbfe-41ab-b723-f3ec89536bc6.jpg",
                        CompanyLogoName = "document_name",
                        QtyDecimalPlaces = 3,
                        ValueDecimalPlaces = 2,
                    },
                    new CompanyMaster
                    {
                        CompanyId = 2,
                        CompanyCode = "CMP002",
                        CompanyName = "Beta Corp",
                        SapVersion = "SAP ERP",
                        PrAllocationLogic = "Material Group",
                        Address = "456 Beta Avenue, Town",
                        CompanyLogoUrl = "logo2.png",
                        CompanyLogoName = "BetaLogo",
                        QtyDecimalPlaces = 3,
                        ValueDecimalPlaces = 2
                    }
                );
            });
        }
    }
}
