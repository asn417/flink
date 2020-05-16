package warehouse.ods.entity;

import java.io.Serializable;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 20:25
 * @Description:收款单事实表
 **/
public class ODS_ReceiptBillVo implements Serializable {

    private static final long serialVersionUID = -8896625263555140603L;

    private String iD;
    private String number;
    private String projectID;
    private String orgID;
    private String roomID;
    private String customerID;
    private String chequeID;
    private String chequeNumber;
    private String tranDate;
    private String revAmount;
    private String receiverID;
    private String billID;
    private String description;
    private String status;
    private String creator;
    private String createTime;
    private String eCID;
    private String period;
    //收款类型，0收款，1退款
    private String businessType;
    private String isVoucher;
    private String cancel;
    private String cancelTime;
    private String isSms;
    private String deleteTime;
    private String isDelete;
    private String printCount;
    private String voucherID;
    private String isInvoiced;
    private String source;
    private String sourceState;
    private String thirdPayBillID;
    private String isRefund;
    private String settEntryName;
    private String isLock;
    private String lockDescription;
    private String isReducePenalty;
    private String isConfirm;
    private String tradeNo;
    private String billNo;
    private String contributors;
    private String contributor;
    private String billType;
    private String overRevAmount;
    private String identifyPeople;
    private String fileIDs;
    private String receiptWriter;
    private String receiptWriteTime;
    private String receiptOperator;
    private String confirmCancelDescription;
    private String firstStage;
    private String confirmReceiptDate;

    public String getiD() {
        return iD;
    }

    public void setiD(String iD) {
        this.iD = iD;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getProjectID() {
        return projectID;
    }

    public void setProjectID(String projectID) {
        this.projectID = projectID;
    }

    public String getOrgID() {
        return orgID;
    }

    public void setOrgID(String orgID) {
        this.orgID = orgID;
    }

    public String getRoomID() {
        return roomID;
    }

    public void setRoomID(String roomID) {
        this.roomID = roomID;
    }

    public String getCustomerID() {
        return customerID;
    }

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }

    public String getChequeID() {
        return chequeID;
    }

    public void setChequeID(String chequeID) {
        this.chequeID = chequeID;
    }

    public String getChequeNumber() {
        return chequeNumber;
    }

    public void setChequeNumber(String chequeNumber) {
        this.chequeNumber = chequeNumber;
    }

    public String getTranDate() {
        return tranDate;
    }

    public void setTranDate(String tranDate) {
        this.tranDate = tranDate;
    }

    public String getRevAmount() {
        return revAmount;
    }

    public void setRevAmount(String revAmount) {
        this.revAmount = revAmount;
    }

    public String getReceiverID() {
        return receiverID;
    }

    public void setReceiverID(String receiverID) {
        this.receiverID = receiverID;
    }

    public String getBillID() {
        return billID;
    }

    public void setBillID(String billID) {
        this.billID = billID;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String geteCID() {
        return eCID;
    }

    public void seteCID(String eCID) {
        this.eCID = eCID;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public String getBusinessType() {
        return businessType;
    }

    public void setBusinessType(String businessType) {
        this.businessType = businessType;
    }

    public String getIsVoucher() {
        return isVoucher;
    }

    public void setIsVoucher(String isVoucher) {
        this.isVoucher = isVoucher;
    }

    public String getCancel() {
        return cancel;
    }

    public void setCancel(String cancel) {
        this.cancel = cancel;
    }

    public String getCancelTime() {
        return cancelTime;
    }

    public void setCancelTime(String cancelTime) {
        this.cancelTime = cancelTime;
    }

    public String getIsSms() {
        return isSms;
    }

    public void setIsSms(String isSms) {
        this.isSms = isSms;
    }

    public String getDeleteTime() {
        return deleteTime;
    }

    public void setDeleteTime(String deleteTime) {
        this.deleteTime = deleteTime;
    }

    public String getIsDelete() {
        return isDelete;
    }

    public void setIsDelete(String isDelete) {
        this.isDelete = isDelete;
    }

    public String getPrintCount() {
        return printCount;
    }

    public void setPrintCount(String printCount) {
        this.printCount = printCount;
    }

    public String getVoucherID() {
        return voucherID;
    }

    public void setVoucherID(String voucherID) {
        this.voucherID = voucherID;
    }

    public String getIsInvoiced() {
        return isInvoiced;
    }

    public void setIsInvoiced(String isInvoiced) {
        this.isInvoiced = isInvoiced;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSourceState() {
        return sourceState;
    }

    public void setSourceState(String sourceState) {
        this.sourceState = sourceState;
    }

    public String getThirdPayBillID() {
        return thirdPayBillID;
    }

    public void setThirdPayBillID(String thirdPayBillID) {
        this.thirdPayBillID = thirdPayBillID;
    }

    public String getIsRefund() {
        return isRefund;
    }

    public void setIsRefund(String isRefund) {
        this.isRefund = isRefund;
    }

    public String getSettEntryName() {
        return settEntryName;
    }

    public void setSettEntryName(String settEntryName) {
        this.settEntryName = settEntryName;
    }

    public String getIsLock() {
        return isLock;
    }

    public void setIsLock(String isLock) {
        this.isLock = isLock;
    }

    public String getLockDescription() {
        return lockDescription;
    }

    public void setLockDescription(String lockDescription) {
        this.lockDescription = lockDescription;
    }

    public String getIsReducePenalty() {
        return isReducePenalty;
    }

    public void setIsReducePenalty(String isReducePenalty) {
        this.isReducePenalty = isReducePenalty;
    }

    public String getIsConfirm() {
        return isConfirm;
    }

    public void setIsConfirm(String isConfirm) {
        this.isConfirm = isConfirm;
    }

    public String getTradeNo() {
        return tradeNo;
    }

    public void setTradeNo(String tradeNo) {
        this.tradeNo = tradeNo;
    }

    public String getBillNo() {
        return billNo;
    }

    public void setBillNo(String billNo) {
        this.billNo = billNo;
    }

    public String getContributors() {
        return contributors;
    }

    public void setContributors(String contributors) {
        this.contributors = contributors;
    }

    public String getContributor() {
        return contributor;
    }

    public void setContributor(String contributor) {
        this.contributor = contributor;
    }

    public String getBillType() {
        return billType;
    }

    public void setBillType(String billType) {
        this.billType = billType;
    }

    public String getOverRevAmount() {
        return overRevAmount;
    }

    public void setOverRevAmount(String overRevAmount) {
        this.overRevAmount = overRevAmount;
    }

    public String getIdentifyPeople() {
        return identifyPeople;
    }

    public void setIdentifyPeople(String identifyPeople) {
        this.identifyPeople = identifyPeople;
    }

    public String getFileIDs() {
        return fileIDs;
    }

    public void setFileIDs(String fileIDs) {
        this.fileIDs = fileIDs;
    }

    public String getReceiptWriter() {
        return receiptWriter;
    }

    public void setReceiptWriter(String receiptWriter) {
        this.receiptWriter = receiptWriter;
    }

    public String getReceiptWriteTime() {
        return receiptWriteTime;
    }

    public void setReceiptWriteTime(String receiptWriteTime) {
        this.receiptWriteTime = receiptWriteTime;
    }

    public String getReceiptOperator() {
        return receiptOperator;
    }

    public void setReceiptOperator(String receiptOperator) {
        this.receiptOperator = receiptOperator;
    }

    public String getConfirmCancelDescription() {
        return confirmCancelDescription;
    }

    public void setConfirmCancelDescription(String confirmCancelDescription) {
        this.confirmCancelDescription = confirmCancelDescription;
    }

    public String getFirstStage() {
        return firstStage;
    }

    public void setFirstStage(String firstStage) {
        this.firstStage = firstStage;
    }

    public String getConfirmReceiptDate() {
        return confirmReceiptDate;
    }

    public void setConfirmReceiptDate(String confirmReceiptDate) {
        this.confirmReceiptDate = confirmReceiptDate;
    }
}
