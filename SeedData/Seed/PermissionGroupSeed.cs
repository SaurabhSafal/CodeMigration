using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    public static class PermissionGroupSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PermissionGroup>(entity =>
            {
                entity.HasData(
                    new PermissionGroup
                    {
                        PermissionGroupId = 1,
                        PermissionGroupName = "Purchase_Requisition",
                        DisplayName = "Requisitions",
                        Icon = "material-symbols:edit-note-outline-rounded",
                        IsActive = true,
                        OrderIndex = 2
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 2,
                        PermissionGroupName = "Events",
                        DisplayName = "Events",
                        Icon = "simple-line-icons:event",
                        IsActive = true,
                        OrderIndex = 3
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 3,
                        PermissionGroupName = "Annual_Rate_Contract",
                        DisplayName = "Contracts",
                        Icon = "hugeicons:contracts",
                        IsActive = true,
                        OrderIndex = 6
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 4,
                        PermissionGroupName = "Note_for_Approval",
                        DisplayName = "Awards",
                        Icon = "material-symbols:trophy-outline-rounded",
                        IsActive = true,
                        OrderIndex = 4
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 5,
                        PermissionGroupName = "Supplier",
                        DisplayName = "Supplier",
                        Icon = "pepicons-print:people",
                        IsActive = true,
                        OrderIndex = 7
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 6,
                        PermissionGroupName = "Purchase_Order",
                        DisplayName = "Orders",
                        Icon = "streamline-ultimate:notes-tasks",
                        IsActive = true,
                        OrderIndex = 5
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 7,
                        PermissionGroupName = "Home",
                        DisplayName = "Home",
                        Icon = "material-symbols:home-outline-rounded",
                        IsActive = true,
                        OrderIndex = 1
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 8,
                        PermissionGroupName = "Users",
                        DisplayName = "Users",
                        Icon = "simple-line-icons:event",
                        IsActive = true,
                        OrderIndex = 8
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 9,
                        PermissionGroupName = "Workflow",
                        DisplayName = "Workflow",
                        Icon = "mdi:workflow",
                        IsActive = true,
                        OrderIndex = 9
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 10,
                        PermissionGroupName = "More",
                        DisplayName = "More",
                        Icon = "circum:square-more",
                        IsActive = true,
                        OrderIndex = 10
                    },
                    new PermissionGroup
                    {
                        PermissionGroupId = 11,
                        PermissionGroupName = "Master",
                        DisplayName = "Master",
                        Icon = "oui:arrow-down",
                        IsActive = true,
                        OrderIndex = 11
                    }        
                );
            });
        }
    }
}
