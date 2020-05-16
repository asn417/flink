package warehouse.ods.entity;

import java.io.Serializable;

/**
 * @Author: wangsen
 * @Date: 2020/4/15 14:11
 * @Description:结算方式维表
 **/
public class ODS_SettlementTypeVo implements Serializable {
    private static final long serialVersionUID = -6951340568965263060L;
    private String iD;
    private String number;
    private String name;
    private String bankID;
    private String account;
    private String isSys;
    private String isEnabled;
    private String isDefault;
    private String description;
    private String eCID;
    private String projectID;
    private String type;
    private String deleteTime;
    private String isDelete;
    private String financialStructureNumber;
    private String bankAccountNumber;
    private String scanningGunType;
    private String bankAccountID;
    private String bankBaseDataID;
    private String payStyle;
    private String payType;
    private String oldID;
    private String bankAccount;
    private String sourceSystem;
    private String sourceID;
    private String isShow;
    private String businessType;
    private String thirdPayId;
    private String tradeType;

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

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getBankID() {
        return this.bankID;
    }

    public void setBankID(final String bankID) {
        this.bankID = bankID;
    }

    public String getAccount() {
        return this.account;
    }

    public void setAccount(final String account) {
        this.account = account;
    }

    public String getIsSys() {
        return this.isSys;
    }

    public void setIsSys(final String isSys) {
        this.isSys = isSys;
    }

    public String getIsEnabled() {
        return this.isEnabled;
    }

    public void setIsEnabled(final String isEnabled) {
        this.isEnabled = isEnabled;
    }

    public String getIsDefault() {
        return this.isDefault;
    }

    public void setIsDefault(final String isDefault) {
        this.isDefault = isDefault;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public String geteCID() {
        return this.eCID;
    }

    public void seteCID(final String eCID) {
        this.eCID = eCID;
    }

    public String getProjectID() {
        return this.projectID;
    }

    public void setProjectID(final String projectID) {
        this.projectID = projectID;
    }

    public String getType() {
        return this.type;
    }

    public void setType(final String type) {
        this.type = type;
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

    public String getFinancialStructureNumber() {
        return this.financialStructureNumber;
    }

    public void setFinancialStructureNumber(final String financialStructureNumber) {
        this.financialStructureNumber = financialStructureNumber;
    }

    public String getBankAccountNumber() {
        return this.bankAccountNumber;
    }

    public void setBankAccountNumber(final String bankAccountNumber) {
        this.bankAccountNumber = bankAccountNumber;
    }

    public String getScanningGunType() {
        return this.scanningGunType;
    }

    public void setScanningGunType(final String scanningGunType) {
        this.scanningGunType = scanningGunType;
    }

    public String getBankAccountID() {
        return this.bankAccountID;
    }

    public void setBankAccountID(final String bankAccountID) {
        this.bankAccountID = bankAccountID;
    }

    public String getBankBaseDataID() {
        return this.bankBaseDataID;
    }

    public void setBankBaseDataID(final String bankBaseDataID) {
        this.bankBaseDataID = bankBaseDataID;
    }

    public String getPayStyle() {
        return this.payStyle;
    }

    public void setPayStyle(final String payStyle) {
        this.payStyle = payStyle;
    }

    public String getPayType() {
        return this.payType;
    }

    public void setPayType(final String payType) {
        this.payType = payType;
    }

    public String getOldID() {
        return this.oldID;
    }

    public void setOldID(final String oldID) {
        this.oldID = oldID;
    }

    public String getBankAccount() {
        return this.bankAccount;
    }

    public void setBankAccount(final String bankAccount) {
        this.bankAccount = bankAccount;
    }

    public String getSourceSystem() {
        return this.sourceSystem;
    }

    public void setSourceSystem(final String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getSourceID() {
        return this.sourceID;
    }

    public void setSourceID(final String sourceID) {
        this.sourceID = sourceID;
    }

    public String getIsShow() {
        return this.isShow;
    }

    public void setIsShow(final String isShow) {
        this.isShow = isShow;
    }

    public String getBusinessType() {
        return this.businessType;
    }

    public void setBusinessType(final String businessType) {
        this.businessType = businessType;
    }

    public String getThirdPayId() {
        return this.thirdPayId;
    }

    public void setThirdPayId(final String thirdPayId) {
        this.thirdPayId = thirdPayId;
    }

    public String getTradeType() {
        return this.tradeType;
    }

    public void setTradeType(final String tradeType) {
        this.tradeType = tradeType;
    }
}
