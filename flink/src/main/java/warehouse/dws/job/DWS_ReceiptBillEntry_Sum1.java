package warehouse.dws.job;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import warehouse.dws.entity.DWS_ReceiptBillEntrySumVo;
import warehouse.dws.sink.DWS_ReceiptBillEntrySum_OPF;
import warehouse.dws.sink.DWS_ReceiptBillEntrySum_Sink;
import warehouse.dws.source.DWS_ReceiptBillEntry_IPF;
import warehouse.dws.source.DWS_ReceiptBillEntry_Source;

/**
 * @Author: wangsen
 * @Date: 2020/5/11 16:06
 * @Description: 从hbase读取dwd_owner_cloud:dwd_receiptBillEntry表，进行聚合汇总
 **/
public class DWS_ReceiptBillEntry_Sum1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<DWD_ReceiptBillEntryVo> dwd_receiptBillEntry = env.addSource(new DWS_ReceiptBillEntry_Source());

        //tEnv.createTemporaryView("dwd_receiptBillEntry",dwd_receiptBillEntry);

        Table table = tEnv.fromDataStream(dwd_receiptBillEntry);

        Table result = table.groupBy("projectID,receiverID,businessType,tranDate,moneyDefineID,settlementTypeID,roomProperty")
                .select("projectID,receiverID,businessType,tranDate,moneyDefineID," +
                        "settlementTypeID,roomProperty,revAmount.sum as totalRevAmount,revPenaltyAmount.sum as totalRevPenaltyAmount");

        //实时更新结果表。
        DataStream<Tuple2<Boolean, DWS_ReceiptBillEntrySumVo>> dataStream = tEnv.toRetractStream(result, DWS_ReceiptBillEntrySumVo.class);


        env.execute("DWS_ReceiptBillEntry_Sum");

    }
}
