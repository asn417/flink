package table.hbase;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import table.dynamic.UserVo;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 14:45
 * @Description:
 **/
public class ReadHbaseByCustomerSource {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        DataStreamSource<UserVo> ods_receiptBill_Source = environment.addSource(new HBaseSource("ods_owner_cloud:ods_receiptBill"), "ods_receiptBill_Source");

        Table ods_receiptBill = tableEnvironment.fromDataStream(ods_receiptBill_Source);

        //DataStreamSource<UserVo> ods_receiptBillEntry_Source = environment.addSource(new HBaseSource("ods_owner_cloud:ods_receiptBillEntry"), "ods_receiptBillEntry_Source");

        //Table ods_receiptBillEntry = tableEnvironment.fromDataStream(ods_receiptBillEntry_Source);

        tableEnvironment.sqlQuery("select * from "+ods_receiptBill);



    }
}
