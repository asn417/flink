package connector.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author: wangsen
 * @Date: 2020/5/8 21:12
 * @Description:
 **/
public class DWD_ReceiptBillEntry_Source extends RichSourceFunction<DWD_ReceiptBillEntryVo> {
    private static final Logger logger = LoggerFactory.getLogger(DWD_ReceiptBillEntry_Source.class);

    private Connection connection = null;
    private PreparedStatement ps = null;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://172.20.181.91/owner_cloud", "wjctest", "CrdWj@201702");//获取连接
        ps = connection.prepareStatement("select DISTINCT entry.FID iD,bill.FID headID,project.Fname projectName,receiver.FEmployeeName receiverName,moneyDefine.FName moneyDefineName,room.FName roomName,settle.FName settlementTypeName\n" +
                "from t_cc_receiptbillentry entry\n" +
                "LEFT JOIN t_cc_receiptbill bill on bill.FID = entry.FHeadID \n" +
                "LEFT JOIN t_bdc_project project on project.FID = entry.FProjectID \n" +
                "LEFT JOIN t_cc_moneydefine moneyDefine on moneyDefine.FID = entry.FMoneyDefineID\n" +
                "LEFT JOIN t_cc_receiptreceiver receiver on receiver.FEmployeeID = bill.FReceiverID \n" +
                "LEFT JOIN t_pc_room room on room.FID = entry.FRoomID\n" +
                "LEFT JOIN t_cc_settlementtype settle on settle.FID = entry.FSettlementTypeID \n" +
                "where entry.FIsDelete = '0' and bill.FIsDelete = '0' and bill.FCancel = '0' and bill.FStatus = '1' and settle.FIsDelete = '0';");
    }

    @Override
    public void run(SourceContext<DWD_ReceiptBillEntryVo> ctx) throws Exception {
        try {

            ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {
                DWD_ReceiptBillEntryVo receiptBillEntryVo = new DWD_ReceiptBillEntryVo();

                receiptBillEntryVo.setiD(resultSet.getString("iD"));
                receiptBillEntryVo.setHeadID(resultSet.getString("headID"));
                receiptBillEntryVo.setProjectName(resultSet.getString("projectName"));
                receiptBillEntryVo.setReceiverName(resultSet.getString("receiverName"));
                receiptBillEntryVo.setMoneyDefineName(resultSet.getString("moneyDefineName"));
                receiptBillEntryVo.setRoomName(resultSet.getString("roomName"));
                receiptBillEntryVo.setSettlementTypeName(resultSet.getString("settlementTypeName"));

                ctx.collect(receiptBillEntryVo);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}
