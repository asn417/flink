package entity.dwd;

import java.io.Serializable;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 19:08
 * @Description:
 **/
public class DWD_ReceiptBillEntryVo implements Serializable {

    private static final long serialVersionUID = -4371903829929722245L;

    //t_cc_receiptbillentry（全部字段）
    private String iD;
    private String number;
    private String headID;
    private String moneyDefineID;
    private String moneyStandardID;
    private String moneyType;
    private String period;
    private String receivableDate;
    private String receivableAmount;
    private String revPenaltyAmount;
    private String revAmount;
    private String description;
    private String businessType;
    private String status;
    private String creator;
    private String createTime;
    private String eCID;
    private String receiveID;
    private String qty;
    private String price;
    private String deleteTime;
    private String isDelete;
    private String taxRate;
    private String taxAmount;
    private String incomeAmount;
    private String offSetMoneydefineID;
    private String month;
    private String ratio;
    private String ladderPriceAndValues;
    private String invoicedAmount;
    private String invoicedID;
    private String invoicedType;
    private String dataSourceType;
    private String isVoucher;
    private String voucherID;
    private String receivePeriod;
    private String settlementTypeID;
    private String reFundAmount;
    private String receiveDate;
    private String lastDosage;
    private String currentDosage;
    private String roomID;
    private String receiveStartDate;
    private String receiveEndDate;
    private String reduPenaltyAmount;
    private String parentEntryID;
    private String range;
    private String insEntryID;
    private String arPenaltyAmount;
    private String storeAmount;
    private String storeBalance;
    private String projectID;
    private String balanceID;
    private String isLock;
    private String billNo;
    private String remark;
    private String isReducePenalty;
    private String receiveCreator;
    private String firstStage;
    private String isInvoiced;
    private String updateTime;

    //t_cc_receiptbill->bill（部分字段）
    private String orgID;
    private String customerID;
    private String chequeID;
    private String chequeNumber;
    private String tranDate;
    private String receiverID;
    private String billID;
    private String cancel;
    private String cancelTime;
    private String isSms;
    private String printCount;
    private String source;
    private String sourceState;
    private String thirdPayBillID;
    private String isRefund;
    private String settEntryName;
    private String lockDescription;
    private String isConfirm;
    private String tradeNo;
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
    private String confirmReceiptDate;
    private String billIsDelete;//FIsDelete
    private String billDeleteTime;//FDeleteTime
    private String billStatus;//FStatus

    //t_bdc_project->project（部分字段）
    private String projectName;

    //t_cc_receiptreceiver->receiver（部分字段）
    private String receiverName;

    //t_cc_moneydefine->moneydefine（部分字段）
    private String moneyDefineName;

    //t_cc_settlementtype->settlementtype（部分字段）
    private String settlementTypeName;
    private String settlementTypeIsDelete;

    //t_pc_room->room（部分字段）
    private String roomName;




    public String getiD() {
        return this.iD;
    }

    public void setiD(final String iD) {
        this.iD = iD;
    }

    public String getNumber() {
        return this.number;
    }

    public void setNumber(final String number) {
        this.number = number;
    }

    public String getHeadID() {
        return this.headID;
    }

    public void setHeadID(final String headID) {
        this.headID = headID;
    }

    public String getMoneyDefineID() {
        return this.moneyDefineID;
    }

    public void setMoneyDefineID(final String moneyDefineID) {
        this.moneyDefineID = moneyDefineID;
    }

    public String getMoneyStandardID() {
        return this.moneyStandardID;
    }

    public void setMoneyStandardID(final String moneyStandardID) {
        this.moneyStandardID = moneyStandardID;
    }

    public String getMoneyType() {
        return this.moneyType;
    }

    public void setMoneyType(final String moneyType) {
        this.moneyType = moneyType;
    }

    public String getPeriod() {
        return this.period;
    }

    public void setPeriod(final String period) {
        this.period = period;
    }

    public String getReceivableDate() {
        return this.receivableDate;
    }

    public void setReceivableDate(final String receivableDate) {
        this.receivableDate = receivableDate;
    }

    public String getReceivableAmount() {
        return this.receivableAmount;
    }

    public void setReceivableAmount(final String receivableAmount) {
        this.receivableAmount = receivableAmount;
    }

    public String getRevPenaltyAmount() {
        return this.revPenaltyAmount;
    }

    public void setRevPenaltyAmount(final String revPenaltyAmount) {
        this.revPenaltyAmount = revPenaltyAmount;
    }

    public String getRevAmount() {
        return this.revAmount;
    }

    public void setRevAmount(final String revAmount) {
        this.revAmount = revAmount;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public String getBusinessType() {
        return this.businessType;
    }

    public void setBusinessType(final String businessType) {
        this.businessType = businessType;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public String getCreator() {
        return this.creator;
    }

    public void setCreator(final String creator) {
        this.creator = creator;
    }

    public String getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(final String createTime) {
        this.createTime = createTime;
    }

    public String geteCID() {
        return this.eCID;
    }

    public void seteCID(final String eCID) {
        this.eCID = eCID;
    }

    public String getReceiveID() {
        return this.receiveID;
    }

    public void setReceiveID(final String receiveID) {
        this.receiveID = receiveID;
    }

    public String getQty() {
        return this.qty;
    }

    public void setQty(final String qty) {
        this.qty = qty;
    }

    public String getPrice() {
        return this.price;
    }

    public void setPrice(final String price) {
        this.price = price;
    }

    public String getDeleteTime() {
        return this.deleteTime;
    }

    public void setDeleteTime(final String deleteTime) {
        this.deleteTime = deleteTime;
    }

    public String getIsDelete() {
        return this.isDelete;
    }

    public void setIsDelete(final String isDelete) {
        this.isDelete = isDelete;
    }

    public String getTaxRate() {
        return this.taxRate;
    }

    public void setTaxRate(final String taxRate) {
        this.taxRate = taxRate;
    }

    public String getTaxAmount() {
        return this.taxAmount;
    }

    public void setTaxAmount(final String taxAmount) {
        this.taxAmount = taxAmount;
    }

    public String getIncomeAmount() {
        return this.incomeAmount;
    }

    public void setIncomeAmount(final String incomeAmount) {
        this.incomeAmount = incomeAmount;
    }

    public String getOffSetMoneydefineID() {
        return this.offSetMoneydefineID;
    }

    public void setOffSetMoneydefineID(final String offSetMoneydefineID) {
        this.offSetMoneydefineID = offSetMoneydefineID;
    }

    public String getMonth() {
        return this.month;
    }

    public void setMonth(final String month) {
        this.month = month;
    }

    public String getRatio() {
        return this.ratio;
    }

    public void setRatio(final String ratio) {
        this.ratio = ratio;
    }

    public String getLadderPriceAndValues() {
        return this.ladderPriceAndValues;
    }

    public void setLadderPriceAndValues(final String ladderPriceAndValues) {
        this.ladderPriceAndValues = ladderPriceAndValues;
    }

    public String getInvoicedAmount() {
        return this.invoicedAmount;
    }

    public void setInvoicedAmount(final String invoicedAmount) {
        this.invoicedAmount = invoicedAmount;
    }

    public String getInvoicedID() {
        return this.invoicedID;
    }

    public void setInvoicedID(final String invoicedID) {
        this.invoicedID = invoicedID;
    }

    public String getInvoicedType() {
        return this.invoicedType;
    }

    public void setInvoicedType(final String invoicedType) {
        this.invoicedType = invoicedType;
    }

    public String getDataSourceType() {
        return this.dataSourceType;
    }

    public void setDataSourceType(final String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getIsVoucher() {
        return this.isVoucher;
    }

    public void setIsVoucher(final String isVoucher) {
        this.isVoucher = isVoucher;
    }

    public String getVoucherID() {
        return this.voucherID;
    }

    public void setVoucherID(final String voucherID) {
        this.voucherID = voucherID;
    }

    public String getReceivePeriod() {
        return this.receivePeriod;
    }

    public void setReceivePeriod(final String receivePeriod) {
        this.receivePeriod = receivePeriod;
    }

    public String getSettlementTypeID() {
        return this.settlementTypeID;
    }

    public void setSettlementTypeID(final String settlementTypeID) {
        this.settlementTypeID = settlementTypeID;
    }

    public String getReFundAmount() {
        return this.reFundAmount;
    }

    public void setReFundAmount(final String reFundAmount) {
        this.reFundAmount = reFundAmount;
    }

    public String getReceiveDate() {
        return this.receiveDate;
    }

    public void setReceiveDate(final String receiveDate) {
        this.receiveDate = receiveDate;
    }

    public String getLastDosage() {
        return this.lastDosage;
    }

    public void setLastDosage(final String lastDosage) {
        this.lastDosage = lastDosage;
    }

    public String getCurrentDosage() {
        return this.currentDosage;
    }

    public void setCurrentDosage(final String currentDosage) {
        this.currentDosage = currentDosage;
    }

    public String getRoomID() {
        return this.roomID;
    }

    public void setRoomID(final String roomID) {
        this.roomID = roomID;
    }

    public String getReceiveStartDate() {
        return this.receiveStartDate;
    }

    public void setReceiveStartDate(final String receiveStartDate) {
        this.receiveStartDate = receiveStartDate;
    }

    public String getReceiveEndDate() {
        return this.receiveEndDate;
    }

    public void setReceiveEndDate(final String receiveEndDate) {
        this.receiveEndDate = receiveEndDate;
    }

    public String getReduPenaltyAmount() {
        return this.reduPenaltyAmount;
    }

    public void setReduPenaltyAmount(final String reduPenaltyAmount) {
        this.reduPenaltyAmount = reduPenaltyAmount;
    }

    public String getParentEntryID() {
        return this.parentEntryID;
    }

    public void setParentEntryID(final String parentEntryID) {
        this.parentEntryID = parentEntryID;
    }

    public String getRange() {
        return this.range;
    }

    public void setRange(final String range) {
        this.range = range;
    }

    public String getInsEntryID() {
        return this.insEntryID;
    }

    public void setInsEntryID(final String insEntryID) {
        this.insEntryID = insEntryID;
    }

    public String getArPenaltyAmount() {
        return this.arPenaltyAmount;
    }

    public void setArPenaltyAmount(final String arPenaltyAmount) {
        this.arPenaltyAmount = arPenaltyAmount;
    }

    public String getStoreAmount() {
        return this.storeAmount;
    }

    public void setStoreAmount(final String storeAmount) {
        this.storeAmount = storeAmount;
    }

    public String getStoreBalance() {
        return this.storeBalance;
    }

    public void setStoreBalance(final String storeBalance) {
        this.storeBalance = storeBalance;
    }

    public String getProjectID() {
        return this.projectID;
    }

    public void setProjectID(final String projectID) {
        this.projectID = projectID;
    }

    public String getBalanceID() {
        return this.balanceID;
    }

    public void setBalanceID(final String balanceID) {
        this.balanceID = balanceID;
    }

    public String getIsLock() {
        return this.isLock;
    }

    public void setIsLock(final String isLock) {
        this.isLock = isLock;
    }

    public String getBillNo() {
        return this.billNo;
    }

    public void setBillNo(final String billNo) {
        this.billNo = billNo;
    }

    public String getRemark() {
        return this.remark;
    }

    public void setRemark(final String remark) {
        this.remark = remark;
    }

    public String getIsReducePenalty() {
        return this.isReducePenalty;
    }

    public void setIsReducePenalty(final String isReducePenalty) {
        this.isReducePenalty = isReducePenalty;
    }

    public String getReceiveCreator() {
        return this.receiveCreator;
    }

    public void setReceiveCreator(final String receiveCreator) {
        this.receiveCreator = receiveCreator;
    }

    public String getFirstStage() {
        return this.firstStage;
    }

    public void setFirstStage(final String firstStage) {
        this.firstStage = firstStage;
    }

    public String getIsInvoiced() {
        return this.isInvoiced;
    }

    public void setIsInvoiced(final String isInvoiced) {
        this.isInvoiced = isInvoiced;
    }

    public String getUpdateTime() {
        return this.updateTime;
    }

    public void setUpdateTime(final String updateTime) {
        this.updateTime = updateTime;
    }

    public String getOrgID() {
        return this.orgID;
    }

    public void setOrgID(final String orgID) {
        this.orgID = orgID;
    }

    public String getCustomerID() {
        return this.customerID;
    }

    public void setCustomerID(final String customerID) {
        this.customerID = customerID;
    }

    public String getChequeID() {
        return this.chequeID;
    }

    public void setChequeID(final String chequeID) {
        this.chequeID = chequeID;
    }

    public String getChequeNumber() {
        return this.chequeNumber;
    }

    public void setChequeNumber(final String chequeNumber) {
        this.chequeNumber = chequeNumber;
    }

    public String getTranDate() {
        return this.tranDate;
    }

    public void setTranDate(final String tranDate) {
        this.tranDate = tranDate;
    }

    public String getReceiverID() {
        return this.receiverID;
    }

    public void setReceiverID(final String receiverID) {
        this.receiverID = receiverID;
    }

    public String getBillID() {
        return this.billID;
    }

    public void setBillID(final String billID) {
        this.billID = billID;
    }

    public String getCancel() {
        return this.cancel;
    }

    public void setCancel(final String cancel) {
        this.cancel = cancel;
    }

    public String getCancelTime() {
        return this.cancelTime;
    }

    public void setCancelTime(final String cancelTime) {
        this.cancelTime = cancelTime;
    }

    public String getIsSms() {
        return this.isSms;
    }

    public void setIsSms(final String isSms) {
        this.isSms = isSms;
    }

    public String getPrintCount() {
        return this.printCount;
    }

    public void setPrintCount(final String printCount) {
        this.printCount = printCount;
    }

    public String getSource() {
        return this.source;
    }

    public void setSource(final String source) {
        this.source = source;
    }

    public String getSourceState() {
        return this.sourceState;
    }

    public void setSourceState(final String sourceState) {
        this.sourceState = sourceState;
    }

    public String getThirdPayBillID() {
        return this.thirdPayBillID;
    }

    public void setThirdPayBillID(final String thirdPayBillID) {
        this.thirdPayBillID = thirdPayBillID;
    }

    public String getIsRefund() {
        return this.isRefund;
    }

    public void setIsRefund(final String isRefund) {
        this.isRefund = isRefund;
    }

    public String getSettEntryName() {
        return this.settEntryName;
    }

    public void setSettEntryName(final String settEntryName) {
        this.settEntryName = settEntryName;
    }

    public String getLockDescription() {
        return this.lockDescription;
    }

    public void setLockDescription(final String lockDescription) {
        this.lockDescription = lockDescription;
    }

    public String getIsConfirm() {
        return this.isConfirm;
    }

    public void setIsConfirm(final String isConfirm) {
        this.isConfirm = isConfirm;
    }

    public String getTradeNo() {
        return this.tradeNo;
    }

    public void setTradeNo(final String tradeNo) {
        this.tradeNo = tradeNo;
    }

    public String getContributors() {
        return this.contributors;
    }

    public void setContributors(final String contributors) {
        this.contributors = contributors;
    }

    public String getContributor() {
        return this.contributor;
    }

    public void setContributor(final String contributor) {
        this.contributor = contributor;
    }

    public String getBillType() {
        return this.billType;
    }

    public void setBillType(final String billType) {
        this.billType = billType;
    }

    public String getOverRevAmount() {
        return this.overRevAmount;
    }

    public void setOverRevAmount(final String overRevAmount) {
        this.overRevAmount = overRevAmount;
    }

    public String getIdentifyPeople() {
        return this.identifyPeople;
    }

    public void setIdentifyPeople(final String identifyPeople) {
        this.identifyPeople = identifyPeople;
    }

    public String getFileIDs() {
        return this.fileIDs;
    }

    public void setFileIDs(final String fileIDs) {
        this.fileIDs = fileIDs;
    }

    public String getReceiptWriter() {
        return this.receiptWriter;
    }

    public void setReceiptWriter(final String receiptWriter) {
        this.receiptWriter = receiptWriter;
    }

    public String getReceiptWriteTime() {
        return this.receiptWriteTime;
    }

    public void setReceiptWriteTime(final String receiptWriteTime) {
        this.receiptWriteTime = receiptWriteTime;
    }

    public String getReceiptOperator() {
        return this.receiptOperator;
    }

    public void setReceiptOperator(final String receiptOperator) {
        this.receiptOperator = receiptOperator;
    }

    public String getConfirmCancelDescription() {
        return this.confirmCancelDescription;
    }

    public void setConfirmCancelDescription(final String confirmCancelDescription) {
        this.confirmCancelDescription = confirmCancelDescription;
    }

    public String getConfirmReceiptDate() {
        return this.confirmReceiptDate;
    }

    public void setConfirmReceiptDate(final String confirmReceiptDate) {
        this.confirmReceiptDate = confirmReceiptDate;
    }

    public String getBillIsDelete() {
        return this.billIsDelete;
    }

    public void setBillIsDelete(final String billIsDelete) {
        this.billIsDelete = billIsDelete;
    }

    public String getBillDeleteTime() {
        return this.billDeleteTime;
    }

    public void setBillDeleteTime(final String billDeleteTime) {
        this.billDeleteTime = billDeleteTime;
    }

    public String getBillStatus() {
        return this.billStatus;
    }

    public void setBillStatus(final String billStatus) {
        this.billStatus = billStatus;
    }

    public String getProjectName() {
        return this.projectName;
    }

    public void setProjectName(final String projectName) {
        this.projectName = projectName;
    }

    public String getReceiverName() {
        return this.receiverName;
    }

    public void setReceiverName(final String receiverName) {
        this.receiverName = receiverName;
    }

    public String getMoneyDefineName() {
        return this.moneyDefineName;
    }

    public void setMoneyDefineName(final String moneyDefineName) {
        this.moneyDefineName = moneyDefineName;
    }

    public String getSettlementTypeName() {
        return this.settlementTypeName;
    }

    public void setSettlementTypeName(final String settlementTypeName) {
        this.settlementTypeName = settlementTypeName;
    }

    public String getSettlementTypeIsDelete() {
        return this.settlementTypeIsDelete;
    }

    public void setSettlementTypeIsDelete(final String settlementTypeIsDelete) {
        this.settlementTypeIsDelete = settlementTypeIsDelete;
    }

    public String getRoomName() {
        return this.roomName;
    }

    public void setRoomName(final String roomName) {
        this.roomName = roomName;
    }
}
