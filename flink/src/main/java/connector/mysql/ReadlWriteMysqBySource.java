package connector.mysql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: wangsen
 * @Date: 2020/5/8 21:10
 * @Description: 通过自定义的source和sink来读写MySQL流式数据
 **/
public class ReadlWriteMysqBySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //fsEnv.setParallelism(6);
        DataStreamSource<DWD_ReceiptBillEntryVo> source = fsEnv.addSource(new DWD_ReceiptBillEntry_Source());

        source.addSink(new DWD_ReceiptBillEntry_Sink());
        fsEnv.execute();
    }
}
