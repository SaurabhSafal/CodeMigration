using System;

namespace SeedData.Constants
{
    public static class Permissions
    {
        public static class PRPermissions
        {
            #region PR Module
            public const string PRDelegation = "PR.Delegation.Full";

            public const string PRDelegationWithRestriction = "PR.Delegation.Restricted";

            public const string CanViewAllPrs = "PR.View.All";

            public const string CanViewPrsWithRestrictions = "PR.View.Restricted";

            public const string CreateTemporaryPR = "PR.Create.Temporary";

            public const string DeletionofTemporaryPR = "PR.Delete.Temporary.Full";

            public const string DeletionofTemporaryPRwithRestriction = "PR.Delete.Temporary.Restricted";

            public const string BulkTemporaryPRUploadDownload = "PR.UploadDownload.BulkTemporary";

            public const string FetchPRFromERP = "PR.Fetch.FromERP";

            public const string CreateRFQPR = "PR.Create.RFQ";

            public const string CreateRepeatPOPR = "PR.Create.RepeatPO";

            public const string CreateARCPOPR = "PR.Create.ARCPO";

            public const string CreateAuctionPR = "PR.Create.Auction";

            #endregion
        }


        public static class EventPermissions
        {
            #region Create Event
            public const string CreateRFQEvent = "Event.Create.PRRFQ";
            public const string CreateAuctionEvent = "Event.Cretae.PRAuction";
            public const string CreateStandAloneRFQEvent = "Event.Create.StandaloneRFQ";
            public const string CreateStandAloneAuctionEvent = "Event.Create.StandaloneAuction";
            public const string UploadDownloadPrLinesRFQ = "Event.Create.UploadDownloadTemplate.RFQ";
            public const string UploadDownloadPRLinesAuction = "Event.Create.UploadDownloadTemplate.Auction";
            #endregion

            #region Event
            public const string ViewAllEvent = "Event.View.All";
            public const string ViewEventWithRestrictions = "Event.View.Restricted";
            public const string CreateEventButton = "Event.Create.button";
            public const string DeleteEventSelf = "Event.Delete.Restricted";
            public const string DeleteEventFull = "Event.Delete.Full";
            public const string TerminateEventRestricted = "Event.Terminate.Restricted";
            public const string TerminateEventFull = "Event.Terminate.Full";
            public const string RecallPartialQty = "Event.RecallPartialQty";
            public const string CopyEvent = "Event.Copy";
            #endregion

            #region Technical Docs
            public const string UploadTechnicalDoc = "Event.Upload.TechnicalDocument";
            public const string UploadTechnicalDocVendorSpecific = "Event.UploadVendorSpecific.TechnicalDocument";
            public const string DeleteTechnicalDoc = "Event.Delete.TechnicalDocument";
            #endregion

            #region Technical Parameters
            public const string AddTechnicalParameter = "Event.Add.TechnicalParameters";
            public const string DeleteTechnicalParameter = "Event.Delete.TechnicalParameters";
            public const string ImportTemplateTechnicalParameterFull = "Event.ImportTemplate.TechnicalParameters.Full";
            public const string ImportTemplateTechnicalParameterRestricted = "Event.ImportTemplate.TechnicalParameters.Restricted";
            public const string UploadDownloadAllLineItems = "Event.UploadDownload.TechnicalParameters";
            #endregion

            #region  Technical Approval Workflow
            public const string AddTechnicalApprovalWorkflow = "Event.Add.TechnicalApproval";
            public const string DeleteTechnicalApprovalWorkflow = "Event.Delete.TechnicalApproval";
            public const string RecallTechnicalApprovalWorkflow = "Event.Recall.TechnicalApproval";
            #endregion

            #region Terms & Conditions
            public const string AddTechnicalTC = "Event.Add.TermsandCondition";
            public const string DeleteTechnicalTC = "Event.Delete.TermsandCondition";
            public const string ImportTemplateTCFull = "Event.ImportTemplate.TermsandCondition";
            public const string UploadDownloadBulkAction = "Event.UploadDownload.TermsandCondition";
            #endregion

            #region Supplier
            public const string AddAssignedEventVendor = "Event.Add.Supplier";
            public const string DeleteAssignedEventVendor = "Event.Delete.Supplier";
            public const string AfterPublishAddAssignedEventVendor = "Event.AddafterPublished.Supplier";
            #endregion

            #region Schedule
            public const string SaveSchedule = "Event.Save.Schedule";
            public const string ChangeSchedule = "Event.SaveafterPublished.Schedule";
            #endregion

            #region Collaboration
            public const string AddCollaboration = "Event.Add.Collaboration";
            public const string DeleteCollaboration = "Event.Delete.Collaboration";
            public const string TransferBuyerCollaboration = "Event.TransferUser.Collaboration";
            #endregion

            #region Items & Price Bid
            public const string AddItem = "Event.AddItem.Pricebid";
            public const string DeleteItem = "Event.DeleteItem.Pricebid";
            public const string ChangeQty = "Event.ChangeQty.Pricebid";
            public const string AddRemarks = "Event.AddExtraColumns.Pricebid";
            public const string SavePriceBid = "Event.Save.Pricebid";
            #endregion

            #region Settings & Comparision
            public const string ChangeSettings = "Event.ChangeSetting";
            public const string PricebidComparision = "Event.PricebidComparision";
            public const string BidOptimization = "Event.BidOptimization";
            public const string SurrogateBidding = "Event.SurrogateBidding";
            public const string DownloadComparision = "Event.DownloadComparision";
            #endregion

            #region Publish
            public const string PublishEvent = "Event.Published";
            #endregion

            #region ARC
            public const string CreateARC = "ARC.Create";
            public const string ViewARCFull = "ARC.View.All";
            public const string ViewARCRestricted = "ARC.View.Restricted";
            public const string DeleteARC = "ARC.Delete";
            public const string RecallARC = "ARC.Recall";
            public const string TerminatedARC = "ARC.Terminate";
            public const string ARCAmendement = "ARC.Amendement";
            #endregion

            #region Next Round
            public const string NextRoundRFQ = "Event.NextRound.RFQ";
            public const string NextRoundAuction = "Event.NextRound.Auction";
            #endregion
        }

        public static class NfaPermissions
        {
            public const string CreateNFA = "Event.Create.NFA";
            public const string RecallNFA = "Event.Recall.NFA";
            public const string DeleteNFA = "Event.Delete.NFA";
            public const string ClarifyNFA = "NFA.Clarification";
            public const string HoldNFA = "NFA.Hold";
            public const string CreatePO = "NFA.CreatePO";
            public const string UpdatePONumber = "NFA.UpdatePONumber";
            public const string DeletePONFA = "NFA.Delete.PO";
        }

        public static class StandAloneNFAPermissions
        {
            public const string CreateStandAloneNFA = "NFA.Create.Standalone";
            public const string DeleteStandAloneNFA = "NFA.Delete.Standalone";
            public const string RecallStandAloneNFA = "NFA.Recall.Standalone";
        }

        public static class AwardListPermissions
        {
            public const string AwardsUnderApprovalListFull = "NFA.UnderApprovalView.All";
            public const string AwardsUnderApprovalListRestricted = "NFA.UnderApprovalView.Restricted";
            public const string AwardsPOPendingListFull = "NFA.POPendingView.All";
            public const string AwardsPOPendingListRestricted = "NFA.POPendingView.Restricted";
            public const string AwardsPOCreatedListFull = "NFA.POCreatedView.All";
            public const string AwardsPOCreatedListRestricted = "NFA.POCreatedView.Restricted";
            public const string AwardsStandAloneListFull = "NFA.StandaloneView.All";
            public const string AwardsStandAloneListRestricted = "NFA.StandaloneView.Restricted";
            public const string AwardsTerminateListFull = "NFA.TerminatedView.All";
            public const string AwardsTerminateListRestricted = "NFA.TerminatedView.Restricted";
        }

        public static class SupplierPermissions
        {
            public const string AddTemporarySupplier = "Supplier.AddTemporary";
            public const string ConvertToRegular = "Supplier.ConverttoRegular";
            public const string DeleteSupplier = "Supplier.Delete";
        }

        public static class POPermissions
        {
            public const string ViewAllPO = "PO.View.All";
            public const string ViewRestrictedPO = "PO.View.Restricted";
            public const string FetchPO = "PO.Fetch";
        }
    }
}
