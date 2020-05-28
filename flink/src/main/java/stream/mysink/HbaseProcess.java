package stream.mysink;

/**
 * @Author: wangsen
 * @Date: 2020/5/27 15:24
 * @Description:
 **/
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stream.TestKafkaCheckpoint;

public class HbaseProcess extends ProcessFunction<String, String> {
    private static Logger logger = LoggerFactory.getLogger(HbaseProcess.class);
    private static final long serialVersionUID = 1L;

    private Connection connection = null;
    private Table table = null;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        try {
            // 加载HBase的配置
            Configuration config = HBaseConfiguration.create();

            Config apolloConfig = ConfigService.getConfig("hbase");
            config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));
            config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
            config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
            connection = ConnectionFactory.createConnection(config);
            TableName tableName = TableName.valueOf("test");
            // 获取表对象
            table = connection.getTable(tableName);

            logger.info("[HbaseSink] : open HbaseSink finished");
        } catch (Exception e) {
            logger.error("[HbaseSink] : open HbaseSink faild {}", e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.info("close...");
        if (null != table) table.close();
        if (null != connection) connection.close();
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            logger.info("[HbaseSink] value={}", value);

            //row1:cf:a:aaa
            String[] split = value.split(":");

            // 创建一个put请求，用于添加数据或者更新数据
            Put put = new Put(Bytes.toBytes(split[0]));
            put.addColumn(Bytes.toBytes(split[1]), Bytes.toBytes(split[2]), Bytes.toBytes(split[3]));
            table.put(put);
            logger.error("[HbaseSink] : put value:{} to hbase", value);
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}

