package connector.mysql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @Author: wangsen
 * @Date: 2020/5/8 13:58
 * @Description: 通过自带的jdbc读写MySQL批量数据（如果设置多并行度，会有重复数据，比如设置并行度为2，则每条数据都会有两条）
 **/
public class ReadWriteMysqlByJDBC {
    public static void main(String[] args) throws Exception {
        //Create a TableEnvironment
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        //需要与获取的字段一一对应，否则会取不到值
        TypeInformation[] fieldTypes = new TypeInformation[] {
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        //读mysql
        DataSet<Row> dataSource = fbEnv.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://172.20.181.91/owner_cloud")
                .setUsername("wjctest")
                .setPassword("CrdWj@201702")
                .setQuery("select DISTINCT entry.FID iD,bill.FID headID,project.Fname projectName,receiver.FEmployeeName receiverName,moneyDefine.FName moneyDefineName,room.FName roomName,settle.FName settlementTypeName\n" +
                        "from t_cc_receiptbillentry entry\n" +
                        "LEFT JOIN t_cc_receiptbill bill on bill.FID = entry.FHeadID \n" +
                        "LEFT JOIN t_bdc_project project on project.FID = entry.FProjectID \n" +
                        "LEFT JOIN t_cc_moneydefine moneyDefine on moneyDefine.FID = entry.FMoneyDefineID\n" +
                        "LEFT JOIN t_cc_receiptreceiver receiver on receiver.FEmployeeID = bill.FReceiverID \n" +
                        "LEFT JOIN t_pc_room room on room.FID = entry.FRoomID\n" +
                        "LEFT JOIN t_cc_settlementtype settle on settle.FID = entry.FSettlementTypeID \n" +
                        "where entry.FIsDelete = '0' and bill.FIsDelete = '0' and bill.FCancel = '0' and bill.FStatus = '1' and settle.FIsDelete = '0';")
                .setRowTypeInfo(rowTypeInfo)
                .finish()).setParallelism(1);

        dataSource.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://172.20.181.91/owner_cloud")
                .setUsername("wjctest")
                .setPassword("CrdWj@201702")
                .setQuery("insert into dwd_receiptbillentry values (?,?,?,?,?,?,?)")
                .finish());//sink只会使用一个slot，就算设置多并行度，也只会使用一个slot

        fbEnv.execute();
    }
}
