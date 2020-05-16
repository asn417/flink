package warehouse.dws.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import warehouse.dws.entity.DWS_ReceiptBillEntrySumVo;
import warehouse.dws.sink.DWS_ReceiptBillEntrySum_OPF;
import warehouse.dws.source.DWS_ReceiptBillEntry_IPF;

/**
 * @Author: wangsen
 * @Date: 2020/5/11 16:06
 * @Description: 从hbase读取dwd_owner_cloud:dwd_receiptBillEntry表，进行聚合汇总
 **/
public class DWS_ReceiptBillEntry_Sum {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<DWD_ReceiptBillEntryVo> dwd_receiptBillEntry = env.createInput(new DWS_ReceiptBillEntry_IPF());
        Table table = tEnv.fromDataSet(dwd_receiptBillEntry);

        table.printSchema();

        Table result = table.groupBy("projectID,receiverID,businessType,tranDate,moneyDefineID,settlementTypeID,roomProperty")
                .select("projectID,receiverID,businessType,tranDate,moneyDefineID," +
                        "settlementTypeID,roomProperty,revAmount.sum as totalRevAmount,revPenaltyAmount.sum as totalRevPenaltyAmount");

        result.printSchema();

        DataSet<DWS_ReceiptBillEntrySumVo> dws_receiptBillEntrySumVoDataSet = tEnv.toDataSet(result, DWS_ReceiptBillEntrySumVo.class);
        long count = dws_receiptBillEntrySumVoDataSet.count();
        System.out.println("============count:"+count);
        TypeInformation<DWS_ReceiptBillEntrySumVo> type = dws_receiptBillEntrySumVoDataSet.getType();
        System.out.println("=============type:"+type);
        //dws_receiptBillEntrySumVoDataSet.print();
        dws_receiptBillEntrySumVoDataSet.output(new DWS_ReceiptBillEntrySum_OPF());

        env.execute("DWS_ReceiptBillEntry_Sum");

    }
}
