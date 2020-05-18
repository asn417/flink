package warehouse.ads.job;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import warehouse.ads.entity.ADS_ReceiptBillEntrySum2Vo;
import warehouse.ads.entity.ADS_ReceiptBillEntrySum3Vo;
import warehouse.ads.sink.ADS_ReceiptBillEntrySum2_OPF;
import warehouse.ads.sink.ADS_ReceiptBillEntrySum3_OPF;
import warehouse.ads.source.ADS_ReceiptBillEntrySum2_IPF;
import warehouse.ads.source.ADS_ReceiptBillEntrySum3_IPF;
import warehouse.dws.entity.DWS_ReceiptBillEntrySumVo;

/**
 * @Author: wangsen
 * @Date: 2020/5/18 9:40
 * @Description:
 **/
public class Dimension3 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<DWS_ReceiptBillEntrySumVo> dwd_receiptBillEntry = env.createInput(new ADS_ReceiptBillEntrySum3_IPF());
        Table table = tEnv.fromDataSet(dwd_receiptBillEntry);

        table.printSchema();

        Table result0 = table.filter("businessType === '0'").groupBy("projectID,tranDate,moneyDefineID,roomProperty")
                .select("projectID,tranDate,moneyDefineID," +
                        "roomProperty,\"0\" as businessType,totalRevAmount.sum as totalRevAmount,totalRevPenaltyAmount.sum as totalRevPenaltyAmount");
        result0.printSchema();

        Table result1 = table.filter("businessType === '1'").groupBy("projectID,tranDate,moneyDefineID,roomProperty")
                .select("projectID,tranDate,moneyDefineID," +
                        "roomProperty,\"1\" as businessType,totalRevAmount.sum as totalRevAmount,totalRevPenaltyAmount.sum as totalRevPenaltyAmount");
        result1.printSchema();

        DataSet<ADS_ReceiptBillEntrySum3Vo> set0 = tEnv.toDataSet(result0, ADS_ReceiptBillEntrySum3Vo.class);
        DataSet<ADS_ReceiptBillEntrySum3Vo> set1 = tEnv.toDataSet(result1, ADS_ReceiptBillEntrySum3Vo.class);
        set0.output(new ADS_ReceiptBillEntrySum3_OPF());
        set1.output(new ADS_ReceiptBillEntrySum3_OPF());
        env.execute();
    }
}
