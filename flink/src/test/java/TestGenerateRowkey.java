import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import utils.MD5Utils;
import utils.RowKeyUtil;

import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/13 12:24
 * @Description:
 **/
public class TestGenerateRowkey {
    @Test
    public void test(){
        System.out.println(MD5Utils.hash("sadaed122e1234dsadasda342dafdaf123rfasfa"));
        System.out.println(RowKeyUtil.generateShortUuid8(MD5Utils.hash("sadaed122e1234dsadasda342dafdaf123rfasfa")));
        System.out.println(RowKeyUtil.generateShortUuid8(MD5Utils.hash("sadaed122e1234dsadasda342dafdaf123rfasfa")));
    }



}
