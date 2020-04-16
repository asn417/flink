package entity.ods;

import java.io.Serializable;

/**
 * @Author: wangsen
 * @Date: 2020/4/15 14:11
 * @Description:收款人维表
 **/
public class ODS_ReceiptReceiverVo implements Serializable {

    private static final long serialVersionUID = 9131595090727123073L;
    private String iD;
    private String employeeID;
    private String employeeName;
    private String projectID;
    private String eCID;
    private String deletetime;
    private String isDelete;

    public String getiD() {
        return this.iD;
    }

    public void setiD(final String iD) {
        this.iD = iD;
    }

    public String getEmployeeID() {
        return this.employeeID;
    }

    public void setEmployeeID(final String employeeID) {
        this.employeeID = employeeID;
    }

    public String getEmployeeName() {
        return this.employeeName;
    }

    public void setEmployeeName(final String employeeName) {
        this.employeeName = employeeName;
    }

    public String getProjectID() {
        return this.projectID;
    }

    public void setProjectID(final String projectID) {
        this.projectID = projectID;
    }

    public String geteCID() {
        return this.eCID;
    }

    public void seteCID(final String eCID) {
        this.eCID = eCID;
    }

    public String getDeletetime() {
        return this.deletetime;
    }

    public void setDeletetime(final String deletetime) {
        this.deletetime = deletetime;
    }

    public String getIsDelete() {
        return this.isDelete;
    }

    public void setIsDelete(final String isDelete) {
        this.isDelete = isDelete;
    }
}
