import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.junit.ClassRule;
import org.junit.Test;
import table.hbase.HBaseConfig;
import utils.HBaseUtil;
import utils.MD5Utils;
import utils.RowKeyUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/13 12:24
 * @Description:
 **/
public class TestGenerateRowkey {
    @Test
    public void test() throws IOException {
        String regex5 = ".*JBErI$";//匹配以JBErI结尾的字符串

        Config config = ConfigService.getConfig("hbase");
        HBaseConfig hBaseConfig = new HBaseConfig(config);
        HBaseUtil.getInstance(hBaseConfig);
        Result result = HBaseUtil.getRow("interface", "91090a80dc4e8200422JBErd");
        System.out.println(result.size());
    }

    @Test
    public void test1(){
        String s = "0.0000000000";
        //System.out.println(new BigDecimal(s).stripTrailingZeros().toPlainString());
        if (new BigDecimal(s).compareTo(BigDecimal.ZERO) == 0){
            System.out.println("=========");
        }
    }

}
