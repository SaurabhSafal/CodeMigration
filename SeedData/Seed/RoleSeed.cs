using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    public static class RoleSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Role>(entity =>
            {
                entity.HasData(
                    new Role { RoleId = 1, Name = "Admin", Description = "Administrator" },
                    new Role { RoleId = 2, Name = "Buyer", Description = "Buyer Role" },
                    new Role { RoleId = 3, Name = "Supplier", Description = "Supplier Role" },
                    new Role { RoleId = 4, Name = "HOD", Description = "HOD Role" },
                    new Role { RoleId = 5, Name = "Technical", Description = "Technical Role" }
                );
            });
        }
    }
}
