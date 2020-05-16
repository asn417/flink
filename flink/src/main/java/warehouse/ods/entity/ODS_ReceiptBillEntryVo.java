package warehouse.ods.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 23:17
 * @Description:实收分录事实表
 **/
public class ODS_ReceiptBillEntryVo implements Serializable {

    private static final long serialVersionUID = -8266549556032180312L;

    private String iD;
    private String number;
    private String headID;
    private String moneyDefineID;
    private String moneyStandardID;
    private String moneyType;
    private String period;
    private String receivableDate;
    private String receivableAmount;
    private BigDecimal revPenaltyAmount;
    private BigDecimal revAmount;
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

    public String getHeadID() {
        return headID;
    }

    public void setHeadID(String headID) {
        this.headID = headID;
    }

    public String getMoneyDefineID() {
        return moneyDefineID;
    }

    public void setMoneyDefineID(String moneyDefineID) {
        this.moneyDefineID = moneyDefineID;
    }

    public String getMoneyStandardID() {
        return moneyStandardID;
    }

    public void setMoneyStandardID(String moneyStandardID) {
        this.moneyStandardID = moneyStandardID;
    }

    public String getMoneyType() {
        return moneyType;
    }

    public void setMoneyType(String moneyType) {
        this.moneyType = moneyType;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public String getReceivableDate() {
        return receivableDate;
    }

    public void setReceivableDate(String receivableDate) {
        this.receivableDate = receivableDate;
    }

    public String getReceivableAmount() {
        return receivableAmount;
    }

    public void setReceivableAmount(String receivableAmount) {
        this.receivableAmount = receivableAmount;
    }

    public BigDecimal getRevPenaltyAmount() {
        return revPenaltyAmount;
    }

    public void setRevPenaltyAmount(BigDecimal revPenaltyAmount) {
        this.revPenaltyAmount = revPenaltyAmount;
    }

    public BigDecimal getRevAmount() {
        return revAmount;
    }

    public void setRevAmount(BigDecimal revAmount) {
        this.revAmount = revAmount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getBusinessType() {
        return businessType;
    }

    public void setBusinessType(String businessType) {
        this.businessType = businessType;
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

    public String getReceiveID() {
        return receiveID;
    }

    public void setReceiveID(String receiveID) {
        this.receiveID = receiveID;
    }

    public String getQty() {
        return qty;
    }

    public void setQty(String qty) {
        this.qty = qty;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
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

    public String getTaxRate() {
        return taxRate;
    }

    public void setTaxRate(String taxRate) {
        this.taxRate = taxRate;
    }

    public String getTaxAmount() {
        return taxAmount;
    }

    public void setTaxAmount(String taxAmount) {
        this.taxAmount = taxAmount;
    }

    public String getIncomeAmount() {
        return incomeAmount;
    }

    public void setIncomeAmount(String incomeAmount) {
        this.incomeAmount = incomeAmount;
    }

    public String getOffSetMoneydefineID() {
        return offSetMoneydefineID;
    }

    public void setOffSetMoneydefineID(String offSetMoneydefineID) {
        this.offSetMoneydefineID = offSetMoneydefineID;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getRatio() {
        return ratio;
    }

    public void setRatio(String ratio) {
        this.ratio = ratio;
    }

    public String getLadderPriceAndValues() {
        return ladderPriceAndValues;
    }

    public void setLadderPriceAndValues(String ladderPriceAndValues) {
        this.ladderPriceAndValues = ladderPriceAndValues;
    }

    public String getInvoicedAmount() {
        return invoicedAmount;
    }

    public void setInvoicedAmount(String invoicedAmount) {
        this.invoicedAmount = invoicedAmount;
    }

    public String getInvoicedID() {
        return invoicedID;
    }

    public void setInvoicedID(String invoicedID) {
        this.invoicedID = invoicedID;
    }

    public String getInvoicedType() {
        return invoicedType;
    }

    public void setInvoicedType(String invoicedType) {
        this.invoicedType = invoicedType;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getIsVoucher() {
        return isVoucher;
    }

    public void setIsVoucher(String isVoucher) {
        this.isVoucher = isVoucher;
    }

    public String getVoucherID() {
        return voucherID;
    }

    public void setVoucherID(String voucherID) {
        this.voucherID = voucherID;
    }

    public String getReceivePeriod() {
        return receivePeriod;
    }

    public void setReceivePeriod(String receivePeriod) {
        this.receivePeriod = receivePeriod;
    }

    public String getSettlementTypeID() {
        return settlementTypeID;
    }

    public void setSettlementTypeID(String settlementTypeID) {
        this.settlementTypeID = settlementTypeID;
    }

    public String getReFundAmount() {
        return reFundAmount;
    }

    public void setReFundAmount(String reFundAmount) {
        this.reFundAmount = reFundAmount;
    }

    public String getReceiveDate() {
        return receiveDate;
    }

    public void setReceiveDate(String receiveDate) {
        this.receiveDate = receiveDate;
    }

    public String getLastDosage() {
        return lastDosage;
    }

    public void setLastDosage(String lastDosage) {
        this.lastDosage = lastDosage;
    }

    public String getCurrentDosage() {
        return currentDosage;
    }

    public void setCurrentDosage(String currentDosage) {
        this.currentDosage = currentDosage;
    }

    public String getRoomID() {
        return roomID;
    }

    public void setRoomID(String roomID) {
        this.roomID = roomID;
    }

    public String getReceiveStartDate() {
        return receiveStartDate;
    }

    public void setReceiveStartDate(String receiveStartDate) {
        this.receiveStartDate = receiveStartDate;
    }

    public String getReceiveEndDate() {
        return receiveEndDate;
    }

    public void setReceiveEndDate(String receiveEndDate) {
        this.receiveEndDate = receiveEndDate;
    }

    public String getReduPenaltyAmount() {
        return reduPenaltyAmount;
    }

    public void setReduPenaltyAmount(String reduPenaltyAmount) {
        this.reduPenaltyAmount = reduPenaltyAmount;
    }

    public String getParentEntryID() {
        return parentEntryID;
    }

    public void setParentEntryID(String parentEntryID) {
        this.parentEntryID = parentEntryID;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public String getInsEntryID() {
        return insEntryID;
    }

    public void setInsEntryID(String insEntryID) {
        this.insEntryID = insEntryID;
    }

    public String getArPenaltyAmount() {
        return arPenaltyAmount;
    }

    public void setArPenaltyAmount(String arPenaltyAmount) {
        this.arPenaltyAmount = arPenaltyAmount;
    }

    public String getStoreAmount() {
        return storeAmount;
    }

    public void setStoreAmount(String storeAmount) {
        this.storeAmount = storeAmount;
    }

    public String getStoreBalance() {
        return storeBalance;
    }

    public void setStoreBalance(String storeBalance) {
        this.storeBalance = storeBalance;
    }

    public String getProjectID() {
        return projectID;
    }

    public void setProjectID(String projectID) {
        this.projectID = projectID;
    }

    public String getBalanceID() {
        return balanceID;
    }

    public void setBalanceID(String balanceID) {
        this.balanceID = balanceID;
    }

    public String getIsLock() {
        return isLock;
    }

    public void setIsLock(String isLock) {
        this.isLock = isLock;
    }

    public String getBillNo() {
        return billNo;
    }

    public void setBillNo(String billNo) {
        this.billNo = billNo;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getIsReducePenalty() {
        return isReducePenalty;
    }

    public void setIsReducePenalty(String isReducePenalty) {
        this.isReducePenalty = isReducePenalty;
    }

    public String getReceiveCreator() {
        return receiveCreator;
    }

    public void setReceiveCreator(String receiveCreator) {
        this.receiveCreator = receiveCreator;
    }

    public String getFirstStage() {
        return firstStage;
    }

    public void setFirstStage(String firstStage) {
        this.firstStage = firstStage;
    }

    public String getIsInvoiced() {
        return isInvoiced;
    }

    public void setIsInvoiced(String isInvoiced) {
        this.isInvoiced = isInvoiced;
    }

    public String getUpdateTime() {
        return this.updateTime;
    }

    public void setUpdateTime(final String updateTime) {
        this.updateTime = updateTime;
    }
}
