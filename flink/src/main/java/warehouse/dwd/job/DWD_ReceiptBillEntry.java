package warehouse.dwd.job;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import warehouse.dwd.sink.DWD_ReceiptBillEntry_Sink;
import warehouse.dwd.source.DWD_ReceiptBillEntry_Source;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wangsen
 * @Date: 2020/5/11 14:05
 * @Description: 更新dwd层的实收分录表ReceiptBillEntry
 * 从mysql获取数据写到hbase
 **/
public class DWD_ReceiptBillEntry {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //从Apollo获取分表信息
        Config apolloConfig = ConfigService.getConfig("ECName_splittable");
        String receiptBill = apolloConfig.getProperty("receiptBill", "41,y1");
        String[] split = receiptBill.split(",");
        List<String> list = new ArrayList<>();
        list.add("");
        for (String str:split){
            list.add("_"+str);
        }
        for (String str:list){
            DataStreamSource<DWD_ReceiptBillEntryVo> source = environment.addSource(new DWD_ReceiptBillEntry_Source(str));
            source.map(new RichMapFunction<DWD_ReceiptBillEntryVo, DWD_ReceiptBillEntryVo>() {
                //第一步：定义累加器
                private IntCounter numLines = new IntCounter();
                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    //第二步：注册累加器
                    getRuntimeContext().addAccumulator(str+":num-lines", numLines);
                }
                @Override
                public DWD_ReceiptBillEntryVo map(DWD_ReceiptBillEntryVo value) throws Exception {
                    //第三步：累加
                    numLines.add(1);
                    return value;
                }
            }).addSink(new DWD_ReceiptBillEntry_Sink());
        }
        JobExecutionResult execute = environment.execute("--------read mysql write hbase--------");
        for (String str:list){
            System.out.println(execute.getAccumulatorResult(str+":num-lines").toString());
        }
    }
}
