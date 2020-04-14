package table.hbase;

import entity.ods.ODS_ReceiptBillEntryVo;
import entity.ods.ODS_ReceiptBillVo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 14:45
 * @Description:
 **/
public class ReadHbaseByCustomerSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        DataStreamSource<ODS_ReceiptBillVo> ods_receiptBill_Source = environment.addSource(new ReceiptBillSource("ods_owner_cloud:ods_receiptBill"), "ods_receiptBill_Source");
        DataStreamSource<ODS_ReceiptBillEntryVo> ods_receiptBillEntry_Source = environment.addSource(new ReceiptBillEntrySource("ods_owner_cloud:ods_receiptBillEntry"), "ods_receiptBillEntry_Source");

        Table ods_receiptBill = tableEnvironment.fromDataStream(ods_receiptBill_Source,"FID as id");//为属性重命名
        Table ods_receiptBillEntry = tableEnvironment.fromDataStream(ods_receiptBillEntry_Source);

        DataStream<Tuple2<Boolean, String>> id = tableEnvironment.toRetractStream(ods_receiptBill.select("id"), String.class);
        id.print("======================id:");


        //Table outerJoin = ods_receiptBillEntry.leftOuterJoin(ods_receiptBill, "FHeadID = id").select("id,FHeadID");
        //Table table = tableEnvironment.sqlQuery("select count(*) from"+outerJoin);

       // TupleTypeInfo<Tuple2<String, String>> tupleType = new TupleTypeInfo<>(Types.STRING,Types.STRING);

        //DataStream<Tuple2<Boolean, Tuple2<String, String>>> tuple2DataStream = tableEnvironment.toRetractStream(outerJoin, tupleType);
        //DataStream<Tuple2<Boolean, Long>> tuple2DataStream = tableEnvironment.toRetractStream(table, Long.class);

        //tuple2DataStream.print();

        environment.execute();

    }
}
