using Microsoft.EntityFrameworkCore;
using SeedData.Models;

namespace Seed
{
    public static class PermissionsTemplateSeed
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PermissionsTemplate>(entity =>
            {
                entity.HasData(
                    // Buyer Role (RoleId = 2) Permissions

                    // PR Module - Buyer
                    new PermissionsTemplate { Id = 1, RoleId = 2, PermissionGroupId = 1, PermissionId = 2 },  // PR.Delegation.Restricted
                    new PermissionsTemplate { Id = 2, RoleId = 2, PermissionGroupId = 1, PermissionId = 4 },  // PR.View.Restricted
                    new PermissionsTemplate { Id = 3, RoleId = 2, PermissionGroupId = 1, PermissionId = 5 },  // PR.Create.Temporary
                    new PermissionsTemplate { Id = 4, RoleId = 2, PermissionGroupId = 1, PermissionId = 7 },  // PR.Delete.Temporary.Restricted
                    new PermissionsTemplate { Id = 5, RoleId = 2, PermissionGroupId = 1, PermissionId = 8 },  // PR.UploadDownload.BulkTemporary
                    new PermissionsTemplate { Id = 6, RoleId = 2, PermissionGroupId = 1, PermissionId = 9 },  // PR.Fetch.FromERP
                    new PermissionsTemplate { Id = 7, RoleId = 2, PermissionGroupId = 1, PermissionId = 10 }, // PR.Create.RFQ
                    new PermissionsTemplate { Id = 8, RoleId = 2, PermissionGroupId = 1, PermissionId = 11 }, // PR.Create.RepeatPO
                    new PermissionsTemplate { Id = 9, RoleId = 2, PermissionGroupId = 1, PermissionId = 12 }, // PR.Create.ARCPO
                    new PermissionsTemplate { Id = 10, RoleId = 2, PermissionGroupId = 1, PermissionId = 13 }, // PR.Create.Auction

                    // Event Module - Buyer
                    new PermissionsTemplate { Id = 11, RoleId = 2, PermissionGroupId = 2, PermissionId = 16 }, // Event.Create.button
                    new PermissionsTemplate { Id = 12, RoleId = 2, PermissionGroupId = 2, PermissionId = 17 }, // Event.Delete.Restricted
                    new PermissionsTemplate { Id = 13, RoleId = 2, PermissionGroupId = 2, PermissionId = 19 }, // Event.RecallPartialQty
                    new PermissionsTemplate { Id = 14, RoleId = 2, PermissionGroupId = 2, PermissionId = 21 }, // Event.Terminate.Restricted
                    new PermissionsTemplate { Id = 15, RoleId = 2, PermissionGroupId = 2, PermissionId = 22 }, // Event.Create.PRRFQ
                    new PermissionsTemplate { Id = 16, RoleId = 2, PermissionGroupId = 2, PermissionId = 23 }, // Event.Cretae.PRAuction
                    new PermissionsTemplate { Id = 17, RoleId = 2, PermissionGroupId = 2, PermissionId = 24 }, // Event.Create.StandaloneRFQ
                    new PermissionsTemplate { Id = 18, RoleId = 2, PermissionGroupId = 2, PermissionId = 25 }, // Event.Create.StandaloneAuction
                    new PermissionsTemplate { Id = 19, RoleId = 2, PermissionGroupId = 2, PermissionId = 26 }, // Event.Create.UploadDownloadTemplate.RFQ
                    new PermissionsTemplate { Id = 20, RoleId = 2, PermissionGroupId = 2, PermissionId = 27 }, // Event.Create.UploadDownloadTemplate.Auction
                    new PermissionsTemplate { Id = 21, RoleId = 2, PermissionGroupId = 2, PermissionId = 28 }, // Event.Copy
                    new PermissionsTemplate { Id = 22, RoleId = 2, PermissionGroupId = 2, PermissionId = 29 }, // Event.Upload.TechnicalDocument
                    new PermissionsTemplate { Id = 23, RoleId = 2, PermissionGroupId = 2, PermissionId = 30 }, // Event.UploadVendorSpecific.TechnicalDocument
                    new PermissionsTemplate { Id = 24, RoleId = 2, PermissionGroupId = 2, PermissionId = 31 }, // Event.Delete.TechnicalDocument
                    new PermissionsTemplate { Id = 25, RoleId = 2, PermissionGroupId = 2, PermissionId = 32 }, // Event.Add.TechnicalParameters
                    new PermissionsTemplate { Id = 26, RoleId = 2, PermissionGroupId = 2, PermissionId = 33 }, // Event.Delete.TechnicalParameters
                    new PermissionsTemplate { Id = 27, RoleId = 2, PermissionGroupId = 2, PermissionId = 35 }, // Event.ImportTemplate.TechnicalParameters.Restricted
                    new PermissionsTemplate { Id = 28, RoleId = 2, PermissionGroupId = 2, PermissionId = 36 }, // Event.UploadDownload.TechnicalParameters
                    new PermissionsTemplate { Id = 29, RoleId = 2, PermissionGroupId = 2, PermissionId = 37 }, // Event.Add.TermsandCondition
                    new PermissionsTemplate { Id = 30, RoleId = 2, PermissionGroupId = 2, PermissionId = 38 }, // Event.Delete.TermsandCondition
                    new PermissionsTemplate { Id = 31, RoleId = 2, PermissionGroupId = 2, PermissionId = 39 }, // Event.ImportTemplate.TermsandCondition
                    new PermissionsTemplate { Id = 32, RoleId = 2, PermissionGroupId = 2, PermissionId = 41 }, // Event.UploadDownload.TermsandCondition
                    new PermissionsTemplate { Id = 33, RoleId = 2, PermissionGroupId = 2, PermissionId = 42 }, // Event.Add.Supplier
                    new PermissionsTemplate { Id = 34, RoleId = 2, PermissionGroupId = 2, PermissionId = 43 }, // Event.Delete.Supplier
                    new PermissionsTemplate { Id = 35, RoleId = 2, PermissionGroupId = 2, PermissionId = 44 }, // Event.AddafterPublished.Supplier
                    new PermissionsTemplate { Id = 36, RoleId = 2, PermissionGroupId = 2, PermissionId = 45 }, // Event.Save.Schedule
                    new PermissionsTemplate { Id = 37, RoleId = 2, PermissionGroupId = 2, PermissionId = 47 }, // Event.SaveafterPublished.Schedule
                    new PermissionsTemplate { Id = 38, RoleId = 2, PermissionGroupId = 2, PermissionId = 48 }, // Event.Add.Collaboration
                    new PermissionsTemplate { Id = 39, RoleId = 2, PermissionGroupId = 2, PermissionId = 49 }, // Event.Delete.Collaboration
                    new PermissionsTemplate { Id = 40, RoleId = 2, PermissionGroupId = 2, PermissionId = 50 }, // Event.TransferUser.Collaboration
                    new PermissionsTemplate { Id = 41, RoleId = 2, PermissionGroupId = 2, PermissionId = 51 }, // Event.AddItem.Pricebid
                    new PermissionsTemplate { Id = 42, RoleId = 2, PermissionGroupId = 2, PermissionId = 52 }, // Event.DeleteItem.Pricebid
                    new PermissionsTemplate { Id = 43, RoleId = 2, PermissionGroupId = 2, PermissionId = 53 }, // Event.ChangeQty.Pricebid
                    new PermissionsTemplate { Id = 44, RoleId = 2, PermissionGroupId = 2, PermissionId = 54 }, // Event.AddExtraColumns.Pricebid
                    new PermissionsTemplate { Id = 45, RoleId = 2, PermissionGroupId = 2, PermissionId = 56 }, // Event.ChangeSetting
                    new PermissionsTemplate { Id = 46, RoleId = 2, PermissionGroupId = 2, PermissionId = 57 }, // Event.Save.Pricebid
                    new PermissionsTemplate { Id = 47, RoleId = 2, PermissionGroupId = 2, PermissionId = 64 }, // Event.Published
                    new PermissionsTemplate { Id = 48, RoleId = 2, PermissionGroupId = 2, PermissionId = 83 }, // Event.Add.TechnicalApproval
                    new PermissionsTemplate { Id = 49, RoleId = 2, PermissionGroupId = 2, PermissionId = 84 }, // Event.Recall.TechnicalApproval
                    new PermissionsTemplate { Id = 50, RoleId = 2, PermissionGroupId = 2, PermissionId = 97 }, // Event.PricebidComparision
                    new PermissionsTemplate { Id = 51, RoleId = 2, PermissionGroupId = 2, PermissionId = 98 }, // Event.BidOptimization
                    new PermissionsTemplate { Id = 52, RoleId = 2, PermissionGroupId = 2, PermissionId = 99 }, // Event.SurrogateBidding
                    new PermissionsTemplate { Id = 53, RoleId = 2, PermissionGroupId = 2, PermissionId = 100 }, // Event.DownloadComparision
                    new PermissionsTemplate { Id = 54, RoleId = 2, PermissionGroupId = 2, PermissionId = 101 }, // Event.Delete.TechnicalApproval

                    // ARC Module - Buyer
                    new PermissionsTemplate { Id = 55, RoleId = 2, PermissionGroupId = 3, PermissionId = 58 }, // ARC.Create
                    new PermissionsTemplate { Id = 56, RoleId = 2, PermissionGroupId = 3, PermissionId = 61 }, // ARC.Delete
                    new PermissionsTemplate { Id = 57, RoleId = 2, PermissionGroupId = 3, PermissionId = 62 }, // ARC.Amendement
                    new PermissionsTemplate { Id = 58, RoleId = 2, PermissionGroupId = 3, PermissionId = 67 }, // ARC.Recall
                    new PermissionsTemplate { Id = 59, RoleId = 2, PermissionGroupId = 3, PermissionId = 86 }, // ARC.Terminate

                    // NFA Module - Buyer
                    new PermissionsTemplate { Id = 60, RoleId = 2, PermissionGroupId = 4, PermissionId = 68 }, // Event.Create.NFA
                    new PermissionsTemplate { Id = 61, RoleId = 2, PermissionGroupId = 4, PermissionId = 69 }, // Event.Recall.NFA
                    new PermissionsTemplate { Id = 62, RoleId = 2, PermissionGroupId = 4, PermissionId = 73 }, // Event.Delete.NFA
                    new PermissionsTemplate { Id = 63, RoleId = 2, PermissionGroupId = 4, PermissionId = 74 }, // NFA.Clarification
                    new PermissionsTemplate { Id = 64, RoleId = 2, PermissionGroupId = 4, PermissionId = 76 }, // NFA.CreatePO
                    new PermissionsTemplate { Id = 65, RoleId = 2, PermissionGroupId = 4, PermissionId = 77 }, // NFA.UpdatePONumber
                    new PermissionsTemplate { Id = 66, RoleId = 2, PermissionGroupId = 4, PermissionId = 79 }, // NFA.Create.Standalone
                    new PermissionsTemplate { Id = 67, RoleId = 2, PermissionGroupId = 4, PermissionId = 81 }, // NFA.Delete.Standalone
                    new PermissionsTemplate { Id = 68, RoleId = 2, PermissionGroupId = 4, PermissionId = 82 }, // NFA.Recall.Standalone

                    // Supplier Module - Buyer
                    new PermissionsTemplate { Id = 69, RoleId = 2, PermissionGroupId = 5, PermissionId = 103 }, // Supplier.AddTemporary
                    new PermissionsTemplate { Id = 70, RoleId = 2, PermissionGroupId = 5, PermissionId = 104 }, // Supplier.ConverttoRegular
                    new PermissionsTemplate { Id = 71, RoleId = 2, PermissionGroupId = 5, PermissionId = 105 }, // Supplier.Delete

                    // PO Module - Buyer
                    new PermissionsTemplate { Id = 72, RoleId = 2, PermissionGroupId = 6, PermissionId = 107 }, // PO.View.Restricted
                    new PermissionsTemplate { Id = 73, RoleId = 2, PermissionGroupId = 6, PermissionId = 108 }, // PO.Fetch

                    // HOD Role (RoleId = 4) Permissions

                    // PR Module - HOD
                    new PermissionsTemplate { Id = 74, RoleId = 4, PermissionGroupId = 1, PermissionId = 1 },  // PR.Delegation.Full
                    new PermissionsTemplate { Id = 75, RoleId = 4, PermissionGroupId = 1, PermissionId = 4 },  // PR.View.Restricted
                    new PermissionsTemplate { Id = 76, RoleId = 4, PermissionGroupId = 1, PermissionId = 5 },  // PR.Create.Temporary
                    new PermissionsTemplate { Id = 77, RoleId = 4, PermissionGroupId = 1, PermissionId = 6 },  // PR.Delete.Temporary.Full
                    new PermissionsTemplate { Id = 78, RoleId = 4, PermissionGroupId = 1, PermissionId = 8 },  // PR.UploadDownload.BulkTemporary
                    new PermissionsTemplate { Id = 79, RoleId = 4, PermissionGroupId = 1, PermissionId = 9 },  // PR.Fetch.FromERP
                    new PermissionsTemplate { Id = 80, RoleId = 4, PermissionGroupId = 1, PermissionId = 10 }, // PR.Create.RFQ
                    new PermissionsTemplate { Id = 81, RoleId = 4, PermissionGroupId = 1, PermissionId = 11 }, // PR.Create.RepeatPO
                    new PermissionsTemplate { Id = 82, RoleId = 4, PermissionGroupId = 1, PermissionId = 12 }, // PR.Create.ARCPO
                    new PermissionsTemplate { Id = 83, RoleId = 4, PermissionGroupId = 1, PermissionId = 13 }, // PR.Create.Auction

                    // Event Module - HOD
                    new PermissionsTemplate { Id = 84, RoleId = 4, PermissionGroupId = 2, PermissionId = 16 }, // Event.Create.button
                    new PermissionsTemplate { Id = 85, RoleId = 4, PermissionGroupId = 2, PermissionId = 18 }, // Event.Terminate.Full
                    new PermissionsTemplate { Id = 86, RoleId = 4, PermissionGroupId = 2, PermissionId = 19 }, // Event.RecallPartialQty
                    new PermissionsTemplate { Id = 87, RoleId = 4, PermissionGroupId = 2, PermissionId = 20 }, // Event.Delete.Full
                    new PermissionsTemplate { Id = 88, RoleId = 4, PermissionGroupId = 2, PermissionId = 22 }, // Event.Create.PRRFQ
                    new PermissionsTemplate { Id = 89, RoleId = 4, PermissionGroupId = 2, PermissionId = 23 }, // Event.Cretae.PRAuction
                    new PermissionsTemplate { Id = 90, RoleId = 4, PermissionGroupId = 2, PermissionId = 24 }, // Event.Create.StandaloneRFQ
                    new PermissionsTemplate { Id = 91, RoleId = 4, PermissionGroupId = 2, PermissionId = 25 }, // Event.Create.StandaloneAuction
                    new PermissionsTemplate { Id = 92, RoleId = 4, PermissionGroupId = 2, PermissionId = 26 }, // Event.Create.UploadDownloadTemplate.RFQ
                    new PermissionsTemplate { Id = 93, RoleId = 4, PermissionGroupId = 2, PermissionId = 27 }, // Event.Create.UploadDownloadTemplate.Auction
                    new PermissionsTemplate { Id = 94, RoleId = 4, PermissionGroupId = 2, PermissionId = 28 }, // Event.Copy
                    new PermissionsTemplate { Id = 95, RoleId = 4, PermissionGroupId = 2, PermissionId = 29 }, // Event.Upload.TechnicalDocument
                    new PermissionsTemplate { Id = 96, RoleId = 4, PermissionGroupId = 2, PermissionId = 30 }, // Event.UploadVendorSpecific.TechnicalDocument
                    new PermissionsTemplate { Id = 97, RoleId = 4, PermissionGroupId = 2, PermissionId = 31 }, // Event.Delete.TechnicalDocument
                    new PermissionsTemplate { Id = 98, RoleId = 4, PermissionGroupId = 2, PermissionId = 32 }, // Event.Add.TechnicalParameters
                    new PermissionsTemplate { Id = 99, RoleId = 4, PermissionGroupId = 2, PermissionId = 33 }, // Event.Delete.TechnicalParameters
                    new PermissionsTemplate { Id = 100, RoleId = 4, PermissionGroupId = 2, PermissionId = 34 }, // Event.ImportTemplate.TechnicalParameters.Full
                    new PermissionsTemplate { Id = 162, RoleId = 4, PermissionGroupId = 2, PermissionId = 35 }, // Event.ImportTemplate.TechnicalParameters.Restricted
                    new PermissionsTemplate { Id = 101, RoleId = 4, PermissionGroupId = 2, PermissionId = 36 }, // Event.UploadDownload.TechnicalParameters
                    new PermissionsTemplate { Id = 102, RoleId = 4, PermissionGroupId = 2, PermissionId = 37 }, // Event.Add.TermsandCondition
                    new PermissionsTemplate { Id = 103, RoleId = 4, PermissionGroupId = 2, PermissionId = 38 }, // Event.Delete.TermsandCondition
                    new PermissionsTemplate { Id = 104, RoleId = 4, PermissionGroupId = 2, PermissionId = 39 }, // Event.ImportTemplate.TermsandCondition
                    new PermissionsTemplate { Id = 105, RoleId = 4, PermissionGroupId = 2, PermissionId = 41 }, // Event.UploadDownload.TermsandCondition
                    new PermissionsTemplate { Id = 106, RoleId = 4, PermissionGroupId = 2, PermissionId = 42 }, // Event.Add.Supplier
                    new PermissionsTemplate { Id = 107, RoleId = 4, PermissionGroupId = 2, PermissionId = 43 }, // Event.Delete.Supplier
                    new PermissionsTemplate { Id = 108, RoleId = 4, PermissionGroupId = 2, PermissionId = 44 }, // Event.AddafterPublished.Supplier
                    new PermissionsTemplate { Id = 109, RoleId = 4, PermissionGroupId = 2, PermissionId = 45 }, // Event.Save.Schedule
                    new PermissionsTemplate { Id = 110, RoleId = 4, PermissionGroupId = 2, PermissionId = 47 }, // Event.SaveafterPublished.Schedule
                    new PermissionsTemplate { Id = 111, RoleId = 4, PermissionGroupId = 2, PermissionId = 48 }, // Event.Add.Collaboration
                    new PermissionsTemplate { Id = 112, RoleId = 4, PermissionGroupId = 2, PermissionId = 49 }, // Event.Delete.Collaboration
                    new PermissionsTemplate { Id = 113, RoleId = 4, PermissionGroupId = 2, PermissionId = 50 }, // Event.TransferUser.Collaboration
                    new PermissionsTemplate { Id = 114, RoleId = 4, PermissionGroupId = 2, PermissionId = 51 }, // Event.AddItem.Pricebid
                    new PermissionsTemplate { Id = 115, RoleId = 4, PermissionGroupId = 2, PermissionId = 52 }, // Event.DeleteItem.Pricebid
                    new PermissionsTemplate { Id = 116, RoleId = 4, PermissionGroupId = 2, PermissionId = 53 }, // Event.ChangeQty.Pricebid
                    new PermissionsTemplate { Id = 117, RoleId = 4, PermissionGroupId = 2, PermissionId = 54 }, // Event.AddExtraColumns.Pricebid
                    new PermissionsTemplate { Id = 118, RoleId = 4, PermissionGroupId = 2, PermissionId = 56 }, // Event.ChangeSetting
                    new PermissionsTemplate { Id = 119, RoleId = 4, PermissionGroupId = 2, PermissionId = 57 }, // Event.Save.Pricebid
                    new PermissionsTemplate { Id = 120, RoleId = 4, PermissionGroupId = 2, PermissionId = 64 }, // Event.Published
                    new PermissionsTemplate { Id = 121, RoleId = 4, PermissionGroupId = 2, PermissionId = 83 }, // Event.Add.TechnicalApproval
                    new PermissionsTemplate { Id = 122, RoleId = 4, PermissionGroupId = 2, PermissionId = 84 }, // Event.Recall.TechnicalApproval
                    new PermissionsTemplate { Id = 123, RoleId = 4, PermissionGroupId = 2, PermissionId = 97 }, // Event.PricebidComparision
                    new PermissionsTemplate { Id = 124, RoleId = 4, PermissionGroupId = 2, PermissionId = 98 }, // Event.BidOptimization
                    new PermissionsTemplate { Id = 125, RoleId = 4, PermissionGroupId = 2, PermissionId = 99 }, // Event.SurrogateBidding
                    new PermissionsTemplate { Id = 126, RoleId = 4, PermissionGroupId = 2, PermissionId = 100 }, // Event.DownloadComparision
                    new PermissionsTemplate { Id = 127, RoleId = 4, PermissionGroupId = 2, PermissionId = 101 }, // Event.Delete.TechnicalApproval

                    // ARC Module - HOD
                    new PermissionsTemplate { Id = 128, RoleId = 4, PermissionGroupId = 3, PermissionId = 58 }, // ARC.Create
                    new PermissionsTemplate { Id = 129, RoleId = 4, PermissionGroupId = 3, PermissionId = 61 }, // ARC.Delete
                    new PermissionsTemplate { Id = 130, RoleId = 4, PermissionGroupId = 3, PermissionId = 62 }, // ARC.Amendement
                    new PermissionsTemplate { Id = 131, RoleId = 4, PermissionGroupId = 3, PermissionId = 67 }, // ARC.Recall
                    new PermissionsTemplate { Id = 132, RoleId = 4, PermissionGroupId = 3, PermissionId = 86 }, // ARC.Terminate

                    // NFA Module - HOD
                    new PermissionsTemplate { Id = 133, RoleId = 4, PermissionGroupId = 4, PermissionId = 68 }, // Event.Create.NFA
                    new PermissionsTemplate { Id = 134, RoleId = 4, PermissionGroupId = 4, PermissionId = 69 }, // Event.Recall.NFA
                    new PermissionsTemplate { Id = 135, RoleId = 4, PermissionGroupId = 4, PermissionId = 73 }, // Event.Delete.NFA
                    new PermissionsTemplate { Id = 136, RoleId = 4, PermissionGroupId = 4, PermissionId = 74 }, // NFA.Clarification
                    new PermissionsTemplate { Id = 137, RoleId = 4, PermissionGroupId = 4, PermissionId = 75 }, // NFA.Hold
                    new PermissionsTemplate { Id = 138, RoleId = 4, PermissionGroupId = 4, PermissionId = 76 }, // NFA.CreatePO
                    new PermissionsTemplate { Id = 139, RoleId = 4, PermissionGroupId = 4, PermissionId = 77 }, // NFA.UpdatePONumber
                    new PermissionsTemplate { Id = 140, RoleId = 4, PermissionGroupId = 4, PermissionId = 78 }, // NFA.Delete.PO
                    new PermissionsTemplate { Id = 141, RoleId = 4, PermissionGroupId = 4, PermissionId = 79 }, // NFA.Create.Standalone
                    new PermissionsTemplate { Id = 142, RoleId = 4, PermissionGroupId = 4, PermissionId = 81 }, // NFA.Delete.Standalone
                    new PermissionsTemplate { Id = 143, RoleId = 4, PermissionGroupId = 4, PermissionId = 82 }, // NFA.Recall.Standalone

                    // Supplier Module - HOD
                    new PermissionsTemplate { Id = 144, RoleId = 4, PermissionGroupId = 5, PermissionId = 103 }, // Supplier.AddTemporary
                    new PermissionsTemplate { Id = 145, RoleId = 4, PermissionGroupId = 5, PermissionId = 104 }, // Supplier.ConverttoRegular
                    new PermissionsTemplate { Id = 146, RoleId = 4, PermissionGroupId = 5, PermissionId = 105 }, // Supplier.Delete

                    // PO Module - HOD
                    new PermissionsTemplate { Id = 147, RoleId = 4, PermissionGroupId = 6, PermissionId = 106 }, // PO.View.All
                    new PermissionsTemplate { Id = 148, RoleId = 4, PermissionGroupId = 6, PermissionId = 108 }, // PO.Fetch

                    // Technical Role (RoleId = 5) Permissions

                    // PR Module - Technical
                    new PermissionsTemplate { Id = 149, RoleId = 5, PermissionGroupId = 1, PermissionId = 4 },  // PR.View.Restricted

                    // Event Module - Technical
                    new PermissionsTemplate { Id = 150, RoleId = 5, PermissionGroupId = 2, PermissionId = 29 }, // Event.Upload.TechnicalDocument
                    new PermissionsTemplate { Id = 151, RoleId = 5, PermissionGroupId = 2, PermissionId = 30 }, // Event.UploadVendorSpecific.TechnicalDocument
                    new PermissionsTemplate { Id = 152, RoleId = 5, PermissionGroupId = 2, PermissionId = 31 }, // Event.Delete.TechnicalDocument
                    new PermissionsTemplate { Id = 153, RoleId = 5, PermissionGroupId = 2, PermissionId = 32 }, // Event.Add.TechnicalParameters
                    new PermissionsTemplate { Id = 154, RoleId = 5, PermissionGroupId = 2, PermissionId = 33 }, // Event.Delete.TechnicalParameters
                    new PermissionsTemplate { Id = 155, RoleId = 5, PermissionGroupId = 2, PermissionId = 34 }, // Event.ImportTemplate.TechnicalParameters.Full
                    new PermissionsTemplate { Id = 156, RoleId = 5, PermissionGroupId = 2, PermissionId = 35 }, // Event.ImportTemplate.TechnicalParameters.Restricted
                    new PermissionsTemplate { Id = 157, RoleId = 5, PermissionGroupId = 2, PermissionId = 36 }, // Event.UploadDownload.TechnicalParameters
                    new PermissionsTemplate { Id = 158, RoleId = 5, PermissionGroupId = 2, PermissionId = 37 }, // Event.Add.TermsandCondition
                    new PermissionsTemplate { Id = 159, RoleId = 5, PermissionGroupId = 2, PermissionId = 38 }, // Event.Delete.TermsandCondition
                    new PermissionsTemplate { Id = 160, RoleId = 5, PermissionGroupId = 2, PermissionId = 39 }, // Event.ImportTemplate.TermsandCondition
                    new PermissionsTemplate { Id = 161, RoleId = 5, PermissionGroupId = 2, PermissionId = 41 }  // Event.UploadDownload.TermsandCondition

                );
            });
        }
    }
}
