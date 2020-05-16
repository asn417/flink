package warehouse.dws.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @Author: wangsen
 * @Date: 2020/4/15 13:47
 * @Description:
 **/
public class DWS_ReceiptBillEntrySumVo implements Serializable {

    private static final long serialVersionUID = -821588195247745307L;
    //项目id
    private String projectID;
    //项目name
    //private String projectName;
    //收款人id
    private String receiverID;
    //收款人name
    //private String receiverName;
    //收款类型
    private String businessType;
    //收款日期
    private String tranDate;
    //款项id
    private String moneyDefineID;
    //款项name
   // private String moneyDefineName;
    //结算方式id
    private String settlementTypeID;
    //结算方式name
    //private String settlementTypeName;
    //业务（房屋属性）属性
    private String roomProperty;

    private BigDecimal totalRevAmount;
    private BigDecimal totalRevPenaltyAmount;

    public String getProjectID() {
        return this.projectID;
    }

    public void setProjectID(final String projectID) {
        this.projectID = projectID;
    }

    public String getReceiverID() {
        return this.receiverID;
    }

    public void setReceiverID(final String receiverID) {
        this.receiverID = receiverID;
    }

    public String getBusinessType() {
        return this.businessType;
    }

    public void setBusinessType(final String businessType) {
        this.businessType = businessType;
    }

    public String getTranDate() {
        return this.tranDate;
    }

    public void setTranDate(final String tranDate) {
        this.tranDate = tranDate;
    }

    public String getMoneyDefineID() {
        return this.moneyDefineID;
    }

    public void setMoneyDefineID(final String moneyDefineID) {
        this.moneyDefineID = moneyDefineID;
    }

    public String getSettlementTypeID() {
        return this.settlementTypeID;
    }

    public void setSettlementTypeID(final String settlementTypeID) {
        this.settlementTypeID = settlementTypeID;
    }

    public String getRoomProperty() {
        return this.roomProperty;
    }

    public void setRoomProperty(final String roomProperty) {
        this.roomProperty = roomProperty;
    }

    public BigDecimal getTotalRevAmount() {
        return this.totalRevAmount;
    }

    public void setTotalRevAmount(final BigDecimal totalRevAmount) {
        this.totalRevAmount = totalRevAmount;
    }

    public BigDecimal getTotalRevPenaltyAmount() {
        return this.totalRevPenaltyAmount;
    }

    public void setTotalRevPenaltyAmount(final BigDecimal totalRevPenaltyAmount) {
        this.totalRevPenaltyAmount = totalRevPenaltyAmount;
    }
}
