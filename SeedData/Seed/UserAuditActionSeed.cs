using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    public static class UserAuditActionSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UserAuditAction>().HasData(
                // Alert Actions
                new UserAuditAction { Id = 1, ActionName = "PR Delegate", ActionDescription = "PR Delegate action", ActionType = "Alert" },
                new UserAuditAction { Id = 2, ActionName = "Auto Assigned PR", ActionDescription = "Auto Assigned PR action", ActionType = "Alert" },
                new UserAuditAction { Id = 3, ActionName = "Add Collaborative User", ActionDescription = "Add Collaborative User action", ActionType = "Alert" },
                new UserAuditAction { Id = 4, ActionName = "Delete Collaborative User", ActionDescription = "Delete Collaborative User action", ActionType = "Alert" },
                new UserAuditAction { Id = 5, ActionName = "Transfer Collaborative User", ActionDescription = "Transfer Collaborative User action", ActionType = "Alert" },
                new UserAuditAction { Id = 6, ActionName = "Assign Technical Approval", ActionDescription = "Assign Technical Approval action", ActionType = "Alert" },
                new UserAuditAction { Id = 7, ActionName = "Send for Approval NFA for Approver", ActionDescription = "Send for Approval NFA for Approver action", ActionType = "Alert" },
                new UserAuditAction { Id = 8, ActionName = "Hold NFA", ActionDescription = "Hold NFA action", ActionType = "Alert" },
                new UserAuditAction { Id = 9, ActionName = "Reject NFA", ActionDescription = "Reject NFA action", ActionType = "Alert" },
                new UserAuditAction { Id = 10, ActionName = "Approve NFA", ActionDescription = "Approve NFA action", ActionType = "Alert" },
                new UserAuditAction { Id = 11, ActionName = "All Level Approved NFA", ActionDescription = "All Level Approved NFA action", ActionType = "Alert" },
                new UserAuditAction { Id = 12, ActionName = "Send for Approval Standalone NFA", ActionDescription = "Send for Approval Standalone NFA action", ActionType = "Alert" },
                new UserAuditAction { Id = 13, ActionName = "After Publish Event Settings change", ActionDescription = "After Publish Event Settings change action", ActionType = "Alert" },
                new UserAuditAction { Id = 14, ActionName = "Event Communication", ActionDescription = "Event Communication action", ActionType = "Alert" },
                new UserAuditAction { Id = 15, ActionName = "Supplier Deviating T&C", ActionDescription = "Supplier deviating T&C action", ActionType = "Alert" },
                new UserAuditAction { Id = 16, ActionName = "Responding to Deviating T&C", ActionDescription = "Responding to deviating T&C action", ActionType = "Alert" },
                new UserAuditAction { Id = 17, ActionName = "Send for Approval ARC", ActionDescription = "Send for Approval ARC action", ActionType = "Alert" },
                new UserAuditAction { Id = 18, ActionName = "Reject ARC", ActionDescription = "Reject ARC action", ActionType = "Alert" },
                new UserAuditAction { Id = 19, ActionName = "Approve ARC", ActionDescription = "Approve ARC action", ActionType = "Alert" },
                new UserAuditAction { Id = 20, ActionName = "All Level Approved ARC", ActionDescription = "All Level Approved ARC action", ActionType = "Alert" },
                new UserAuditAction { Id = 46, ActionName = "NFA Clarification", ActionDescription = "NFA Clarification action", ActionType = "Alert" },
                new UserAuditAction { Id = 48, ActionName = "Terminate ARC", ActionDescription = "Terminate ARC action", ActionType = "Alert" },


                // Notification Actions
                new UserAuditAction { Id = 21, ActionName = "Create Event", ActionDescription = "Create Event action", ActionType = "Notification" },
                new UserAuditAction { Id = 22, ActionName = "Terminate Event", ActionDescription = "Terminate Event action", ActionType = "Notification" },
                new UserAuditAction { Id = 23, ActionName = "Recall-Partial Qty", ActionDescription = "Recall-Partial Qty action", ActionType = "Notification" },
                new UserAuditAction { Id = 24, ActionName = "After Publish add and Delete supplier", ActionDescription = "After Publish add and Delete supplier action", ActionType = "Notification" },
                new UserAuditAction { Id = 25, ActionName = "After Publish Change Schedule", ActionDescription = "After Publish Change Schedule action", ActionType = "Notification" },
                new UserAuditAction { Id = 26, ActionName = "Recall Technical Approval", ActionDescription = "Recall Technical Approval action", ActionType = "Notification" },
                new UserAuditAction { Id = 27, ActionName = "Publish Event", ActionDescription = "Publish Event action", ActionType = "Notification" },
                new UserAuditAction { Id = 28, ActionName = "Next Round", ActionDescription = "Next Round action", ActionType = "Notification" },
                new UserAuditAction { Id = 29, ActionName = "Bid Optimization", ActionDescription = "Bid Optimization action", ActionType = "Notification" },
                new UserAuditAction { Id = 30, ActionName = "Send for Approval NFA for Reporting Manager", ActionDescription = "Send for Approval NFA for Reporting Manager action", ActionType = "Notification" },
                new UserAuditAction { Id = 31, ActionName = "Recall NFA", ActionDescription = "Recall NFA action", ActionType = "Notification" },
                new UserAuditAction { Id = 32, ActionName = "Update PO Number", ActionDescription = "Update PO Number action", ActionType = "Notification" },
                new UserAuditAction { Id = 33, ActionName = "Send for Approval Standalone NFA", ActionDescription = "Send for Approval Standalone NFA action", ActionType = "Notification" },
                new UserAuditAction { Id = 34, ActionName = "Create PO", ActionDescription = "Create PO action", ActionType = "Notification" },
                new UserAuditAction { Id = 35, ActionName = "After Publish Upload Technical Doc by Collaborative User", ActionDescription = "After Publish Upload Technical Doc by Collaborative User action", ActionType = "Notification" },
                new UserAuditAction { Id = 36, ActionName = "Supplier Participate in Event", ActionDescription = "Supplier Participate in Event action", ActionType = "Notification" },
                new UserAuditAction { Id = 37, ActionName = "Supplier Regret in Event", ActionDescription = "Supplier Regret in Event action", ActionType = "Notification" },
                new UserAuditAction { Id = 38, ActionName = "Supplier Accepting T&C", ActionDescription = "Supplier deviating T&C action", ActionType = "Notification" },
                new UserAuditAction { Id = 39, ActionName = "Supplier Upload Doc", ActionDescription = "Supplier Upload Doc action", ActionType = "Notification" },
                new UserAuditAction { Id = 40, ActionName = "Supplier Submit Bid", ActionDescription = "Supplier Submit Bid action", ActionType = "Notification" },
                new UserAuditAction { Id = 41, ActionName = "Buyer Responding to Deviating T&C", ActionDescription = "Buyer Responding to Deviating T&C action", ActionType = "Notification" },
                new UserAuditAction { Id = 42, ActionName = "Send for Approval ARC", ActionDescription = "Send for Approval ARC action", ActionType = "Notification" },
                new UserAuditAction { Id = 43, ActionName = "Recall ARC", ActionDescription = "Recall ARC action", ActionType = "Notification" },
                new UserAuditAction { Id = 44, ActionName = "Approve ARC", ActionDescription = "Approve ARC action", ActionType = "Notification" },
                new UserAuditAction { Id = 45, ActionName = "Convert to Regular Vendor", ActionDescription = "Convert Temp to Regular Vendor action", ActionType = "Notification" },
                new UserAuditAction { Id = 47, ActionName = "Terminate NFA", ActionDescription = "Terminate NFA action", ActionType = "Notification" },

                // Additional Actions (null ActionType)
                new UserAuditAction { Id = 49, ActionName = "NFA Deleted", ActionDescription = "NFA Deleted action", ActionType = null },
                new UserAuditAction { Id = 50, ActionName = "Update Deviation-Term", ActionDescription = "Update Deviation-Term Remarks action", ActionType = null },
                new UserAuditAction { Id = 51, ActionName = "Event Deleted", ActionDescription = "Event Deleted action", ActionType = null }
            );
        }
    }
}
