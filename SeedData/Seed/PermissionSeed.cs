using Microsoft.EntityFrameworkCore;
using SeedData.Models;
using SeedData.Constants;   

namespace Seed
{
    public static class PermissionSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Permission>(entity =>
            {
                entity.HasData(
                    // ---------------- PR Module ----------------
                    new Permission
                    {
                        PermissionId = 1,
                        PermissionName = "PR.Delegation.Full",
                        Description = "Can delegate any PR",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 2,
                        PermissionName = "PR.Delegation.Restricted",
                        Description = "Can delegate only within assigned scope",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 3,
                        PermissionName = "PR.View.All",
                        Description = "Can view all PRs regardless of allocation",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 4,
                        PermissionName = "PR.View.Restricted",
                        Description = "Can view PRs based on plant/companycode",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 5,
                        PermissionName = "PR.Create.Temporary",
                        Description = "Can create a temporary PR for ad-hoc need",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 6,
                        PermissionName = "PR.Delete.Temporary.Full",
                        Description = "Can delete any Temporary PR",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 7,
                        PermissionName = "PR.Delete.Temporary.Restricted",
                        Description = "Can delete only within assigned scope",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 8,
                        PermissionName = "PR.UploadDownload.BulkTemporary",
                        Description = "Can create a Bulk temporary PR for ad-hoc need",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 9,
                        PermissionName = "PR.Fetch.FromERP",
                        Description = "Pull PR by PR Number from external system (e.g., SAP)",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 10,
                        PermissionName = "PR.Create.RFQ",
                        Description = "Can initiate RFQ from an approved PR",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 11,
                        PermissionName = "PR.Create.RepeatPO",
                        Description = "Can create Repeat PO from past PR",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 12,
                        PermissionName = "PR.Create.ARCPO",
                        Description = "Can create ARC PO from PR",
                        PermissionGroupId = 1
                    },
                    new Permission
                    {
                        PermissionId = 13,
                        PermissionName = "PR.Create.Auction",
                        Description = "Can initiate Auction from PR",
                        PermissionGroupId = 1
                    },

                    // ---------------- Events Module ----------------
                    new Permission
                    {
                        PermissionId = 14,
                        PermissionName = Permissions.EventPermissions.ViewAllEvent,
                        Description = "View All Events (Based on Company Access)",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 15,
                        PermissionName = Permissions.EventPermissions.ViewEventWithRestrictions,
                        Description = "View All Events (Based on Company + Plant Access)",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 16,
                        PermissionName = Permissions.EventPermissions.CreateEventButton,
                        Description = "Can create RFQ event for assigned PR",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 17,
                        PermissionName = Permissions.EventPermissions.DeleteEventSelf,
                        Description = "Can delete event if user created it",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 18,
                        PermissionName = Permissions.EventPermissions.TerminateEventFull,
                        Description = "Terminate events based on Company Access",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 19,
                        PermissionName = Permissions.EventPermissions.RecallPartialQty,
                        Description = "Can recall partial quantities",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 20,
                        PermissionName = Permissions.EventPermissions.DeleteEventFull,
                        Description = "Can delete events based on Company Access",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 21,
                        PermissionName = Permissions.EventPermissions.TerminateEventRestricted,
                        Description = "Can terminate any event if user created it",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 22,
                        PermissionName = Permissions.EventPermissions.CreateRFQEvent,
                        Description = "Can initiate RFQ from an approved PR",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 23,
                        PermissionName = Permissions.EventPermissions.CreateAuctionEvent,
                        Description = "Can initiate Auction from PR",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 24,
                        PermissionName = Permissions.EventPermissions.CreateStandAloneRFQEvent,
                        Description = "Can create Stand-alone RFQ from Item Master or Stand-alone Items",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 25,
                        PermissionName = Permissions.EventPermissions.CreateStandAloneAuctionEvent,
                        Description = "Can create Stand-alone Auction from Item Master or Stand-alone Items",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 26,
                        PermissionName = Permissions.EventPermissions.UploadDownloadPrLinesRFQ,
                        Description = "Can create RFQ from SAP PR Lines Template",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 27,
                        PermissionName = Permissions.EventPermissions.UploadDownloadPRLinesAuction,
                        Description = "Can create Auction from SAP PR Lines Template",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 28,
                        PermissionName = Permissions.EventPermissions.CopyEvent,
                        Description = "Can copy details from the Past Event",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 29,
                        PermissionName = Permissions.EventPermissions.UploadTechnicalDoc,
                        Description = "Can upload Technical Doc",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 30,
                        PermissionName = Permissions.EventPermissions.UploadTechnicalDocVendorSpecific,
                        Description = "Can upload Technical Doc Vendor Specific",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 31,
                        PermissionName = Permissions.EventPermissions.DeleteTechnicalDoc,
                        Description = "If User has rights of upload doc then only del button should be visible",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 32,
                        PermissionName = Permissions.EventPermissions.AddTechnicalParameter,
                        Description = "Can add technical Parameter",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 33,
                        PermissionName = Permissions.EventPermissions.DeleteTechnicalParameter,
                        Description = "Can del the Technical Parameter",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 34,
                        PermissionName = Permissions.EventPermissions.ImportTemplateTechnicalParameterFull,
                        Description = "Can add Global templates",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 35,
                        PermissionName = Permissions.EventPermissions.ImportTemplateTechnicalParameterRestricted,
                        Description = "Based on User created only",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 36,
                        PermissionName = Permissions.EventPermissions.UploadDownloadAllLineItems,
                        Description = "Can use template for the Bulk uploation",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 37,
                        PermissionName = Permissions.EventPermissions.AddTechnicalTC,
                        Description = "Can add T&C",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 38,
                        PermissionName = Permissions.EventPermissions.DeleteTechnicalTC,
                        Description = "Can del the T&C",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 39,
                        PermissionName = Permissions.EventPermissions.ImportTemplateTCFull,
                        Description = "Can add Global templates",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 41,
                        PermissionName = Permissions.EventPermissions.UploadDownloadBulkAction,
                        Description = "Download Upload for Bulk Action",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 42,
                        PermissionName = Permissions.EventPermissions.AddAssignedEventVendor,
                        Description = "Can add supplier",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 43,
                        PermissionName = Permissions.EventPermissions.DeleteAssignedEventVendor,
                        Description = "Can del supplier",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 44,
                        PermissionName = Permissions.EventPermissions.AfterPublishAddAssignedEventVendor,
                        Description = "Can add supplier after Publish",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 45,
                        PermissionName = Permissions.EventPermissions.SaveSchedule,
                        Description = "Can Save the Schedule",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 47,
                        PermissionName = Permissions.EventPermissions.ChangeSchedule,
                        Description = "Can change schedule before and after event close",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 48,
                        PermissionName = Permissions.EventPermissions.AddCollaboration,
                        Description = "Can add Collaborative User",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 49,
                        PermissionName = Permissions.EventPermissions.DeleteCollaboration,
                        Description = "Can delete Collaborative User",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 50,
                        PermissionName = Permissions.EventPermissions.TransferBuyerCollaboration,
                        Description = "Can transfer Collaborative User",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 51,
                        PermissionName = Permissions.EventPermissions.AddItem,
                        Description = "Can add additional Items",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 52,
                        PermissionName = Permissions.EventPermissions.DeleteItem,
                        Description = "Can delete selected items",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 53,
                        PermissionName = Permissions.EventPermissions.ChangeQty,
                        Description = "can change qty",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 54,
                        PermissionName = Permissions.EventPermissions.AddRemarks,
                        Description = "Can add other remarks columns",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 56,
                        PermissionName = Permissions.EventPermissions.ChangeSettings,
                        Description = "PriceBid + Auction Setting",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 57,
                        PermissionName = Permissions.EventPermissions.SavePriceBid,
                        Description = "Save Button",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 64,
                        PermissionName = Permissions.EventPermissions.PublishEvent,
                        Description = "Event Publish",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 83,
                        PermissionName = Permissions.EventPermissions.AddTechnicalApprovalWorkflow,
                        Description = "Add Technical Approval Workflow",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 84,
                        PermissionName = Permissions.EventPermissions.RecallTechnicalApprovalWorkflow,
                        Description = "Recall Technical Approval Workflow",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 97,
                        PermissionName = Permissions.EventPermissions.PricebidComparision,
                        Description = "Event Pricebid Comparision",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 98,
                        PermissionName = Permissions.EventPermissions.BidOptimization,
                        Description = "Event Bid Optimization",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 99,
                        PermissionName = Permissions.EventPermissions.SurrogateBidding,
                        Description = "Event Surrogate Bidding",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 100,
                        PermissionName = Permissions.EventPermissions.DownloadComparision,
                        Description = "Event Download Comparision",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 101,
                        PermissionName = Permissions.EventPermissions.DeleteTechnicalApprovalWorkflow,
                        Description = "Delete Technical Approval Workflow",
                        PermissionGroupId = 2
                    },

                    // ---------------- ARC Module ----------------
                    new Permission
                    {
                        PermissionId = 58,
                        PermissionName = Permissions.EventPermissions.CreateARC,
                        Description = "Create ARC",
                        PermissionGroupId = 3
                    },
                    new Permission
                    {
                        PermissionId = 60,
                        PermissionName = Permissions.EventPermissions.ViewARCFull,
                        Description = "View ARC - Full Access",
                        PermissionGroupId = 3
                    },
                    new Permission
                    {
                        PermissionId = 61,
                        PermissionName = Permissions.EventPermissions.DeleteARC,
                        Description = "Delete ARC",
                        PermissionGroupId = 3
                    },
                    new Permission
                    {
                        PermissionId = 62,
                        PermissionName = Permissions.EventPermissions.ARCAmendement,
                        Description = "ARC Amendement",
                        PermissionGroupId = 3
                    },
                    new Permission
                    {
                        PermissionId = 67,
                        PermissionName = Permissions.EventPermissions.RecallARC,
                        Description = "Recall ARC",
                        PermissionGroupId = 3
                    },
                    new Permission
                    {
                        PermissionId = 86,
                        PermissionName = Permissions.EventPermissions.TerminatedARC,
                        Description = "Terminated ARC",
                        PermissionGroupId = 3
                    },
                    new Permission
                    {
                        PermissionId = 102,
                        PermissionName = Permissions.EventPermissions.ViewARCRestricted,
                        Description = "View ARC - Restricted Access",
                        PermissionGroupId = 3
                    },                    // ---------------- NFA Module ----------------
                    new Permission
                    {
                        PermissionId = 68,
                        PermissionName =Permissions.NfaPermissions.CreateNFA,
                        Description = "Create NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 69,
                        PermissionName =Permissions.NfaPermissions.RecallNFA,
                        Description = "Recall NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 73,
                        PermissionName =Permissions.NfaPermissions.DeleteNFA,
                        Description = "Delete NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 74,
                        PermissionName =Permissions.NfaPermissions.ClarifyNFA,
                        Description = "Clarify NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 75,
                        PermissionName =Permissions.NfaPermissions.HoldNFA,
                        Description = "Hold NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 76,
                        PermissionName =Permissions.NfaPermissions.CreatePO,
                        Description = "Create PO for NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 77,
                        PermissionName =Permissions.NfaPermissions.UpdatePONumber,
                        Description = "Update PO Number for NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 78,
                        PermissionName =Permissions.NfaPermissions.DeletePONFA,
                        Description = "Delete PO for NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 79,
                        PermissionName = Permissions.StandAloneNFAPermissions.CreateStandAloneNFA,
                        Description = "Create StandAlone NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 81,
                        PermissionName = Permissions.StandAloneNFAPermissions.DeleteStandAloneNFA,
                        Description = "Delete StandAlone NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 82,
                        PermissionName = Permissions.StandAloneNFAPermissions.RecallStandAloneNFA,
                        Description = "Recall StandAlone NFA",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 87,
                        PermissionName = Permissions.AwardListPermissions.AwardsUnderApprovalListFull,
                        Description = "Awards Under approval List - View All company NFA - Full Rights - Based on Company Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 88,
                        PermissionName = Permissions.AwardListPermissions.AwardsUnderApprovalListRestricted,
                        Description = "Awards Under approval List - View All Company + Plant NFA - Restricted Rights - Based on Company + Plant Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 89,
                        PermissionName = Permissions.AwardListPermissions.AwardsPOPendingListFull,
                        Description = "Awards PO Pending List - View All company NFA - Full Rights - Based on Company Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 90,
                        PermissionName = Permissions.AwardListPermissions.AwardsPOPendingListRestricted,
                        Description = "Awards PO Pending List - View All Company + Plant NFA - Restricted Rights - Based on Company + Plant Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 91,
                        PermissionName = Permissions.AwardListPermissions.AwardsPOCreatedListFull,
                        Description = "Awards PO Created List - View All company NFA - Full Rights - Based on Company Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 92,
                        PermissionName = Permissions.AwardListPermissions.AwardsPOCreatedListRestricted,
                        Description = "Awards PO Created List - View All Company + Plant NFA - Restricted Rights - Based on Company + Plant Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 93,
                        PermissionName = Permissions.AwardListPermissions.AwardsStandAloneListFull,
                        Description = "Awards Stand alone List - View All company NFA - Full Rights - Based on Company Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 94,
                        PermissionName = Permissions.AwardListPermissions.AwardsStandAloneListRestricted,
                        Description = "Awards Stand alone List - View All Company + Plant NFA - Restricted Rights - Based on Company + Plant Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 95,
                        PermissionName = Permissions.AwardListPermissions.AwardsTerminateListFull,
                        Description = "Awards Terminate List - View All company NFA - Full Rights - Based on Company Access",
                        PermissionGroupId = 4
                    },
                    new Permission
                    {
                        PermissionId = 96,
                        PermissionName = Permissions.AwardListPermissions.AwardsTerminateListRestricted,
                        Description = "Awards Terminate List - View All Company + Plant NFA - Restricted Rights - Based on Company + Plant Access",
                        PermissionGroupId = 4
                    },

                    // ---------------- Supplier Module ----------------
                    new Permission
                    {
                        PermissionId = 103,
                        PermissionName = Permissions.SupplierPermissions.AddTemporarySupplier,
                        Description = "Add Temporary Supplier",
                        PermissionGroupId = 5
                    },
                    new Permission
                    {
                        PermissionId = 104,
                        PermissionName = Permissions.SupplierPermissions.ConvertToRegular,
                        Description = "Convert Temporary Supplier to Regular",
                        PermissionGroupId = 5
                    },
                    new Permission
                    {
                        PermissionId = 105,
                        PermissionName = Permissions.SupplierPermissions.DeleteSupplier,
                        Description = "Delete Supplier",
                        PermissionGroupId = 5
                    },

                    // ---------------- PO Module ----------------
                    new Permission
                    {
                        PermissionId = 106,
                        PermissionName = Permissions.POPermissions.ViewAllPO,
                        Description = "View All PO - Full Access",
                        PermissionGroupId = 6
                    },
                    new Permission
                    {
                        PermissionId = 107,
                        PermissionName = Permissions.POPermissions.ViewRestrictedPO,
                        Description = "View PO - Restricted Access",
                        PermissionGroupId = 6
                    },
                    new Permission
                    {
                        PermissionId = 108,
                        PermissionName = Permissions.POPermissions.FetchPO,
                        Description = "Fetch PO from ERP",
                        PermissionGroupId = 6
                    },
                    new Permission
                    {
                        PermissionId = 109,
                        PermissionName = Permissions.EventPermissions.NextRoundRFQ,
                        Description = "Next Round RFQ",
                        PermissionGroupId = 2
                    },
                    new Permission
                    {
                        PermissionId = 110,
                        PermissionName = Permissions.EventPermissions.NextRoundAuction,
                        Description = "Next Round Auction",
                        PermissionGroupId = 2
                    }
                );
            });
        }
    }
}
