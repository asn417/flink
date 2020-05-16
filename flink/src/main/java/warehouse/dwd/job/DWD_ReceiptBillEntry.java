package warehouse.dwd.job;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import warehouse.dwd.sink.DWD_ReceiptBillEntry_Sink;
import warehouse.dwd.source.DWD_ReceiptBillEntry_Source;

/**
 * @Author: wangsen
 * @Date: 2020/5/11 14:05
 * @Description: 更新dwd层的实收分录表ReceiptBillEntry
 * 从mysql获取数据写到hbase
 **/
public class DWD_ReceiptBillEntry {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<DWD_ReceiptBillEntryVo> source = environment.addSource(new DWD_ReceiptBillEntry_Source());

        source.addSink(new DWD_ReceiptBillEntry_Sink());

        environment.execute("--------read mysql write hbase--------");
    }
}
