package Flink;

/**
 * @Author: wangsen
 * @Date: 2020/5/30 17:23
 * @Description:
 **/
public class TransferBillVo {

    //mysql中的表名，适配分表
    private String table;

    private String id;
    private String projectID;
    private String ecID;
    private String roomID;
    private String customerID;
    private String moneyDefineID;
    private String transferType;
    private String period;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getID() {
        return id;
    }

    public void setID(String id) {
        this.id = id;
    }

    public String getProjectID() {
        return projectID;
    }

    public void setProjectID(String projectID) {
        this.projectID = projectID;
    }

    public String getEcID() {
        return ecID;
    }

    public void setEcID(String ecID) {
        this.ecID = ecID;
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

    public String getMoneyDefineID() {
        return moneyDefineID;
    }

    public void setMoneyDefineID(String moneyDefineID) {
        this.moneyDefineID = moneyDefineID;
    }

    public String getTransferType() {
        return transferType;
    }

    public void setTransferType(String transferType) {
        this.transferType = transferType;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }
}
