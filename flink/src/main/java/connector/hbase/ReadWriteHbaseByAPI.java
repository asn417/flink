package connector.hbase;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import table.hbase.HBaseConfig;

import java.io.IOException;

/**
 * 通过flink-hbase依赖读写hbase
 */
public class ReadWriteHbaseByAPI {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        byte[] cf = "info".getBytes();
        byte[] cq = "name".getBytes();
        environment.createInput(new TableInputFormat<Tuple2<String,String>>() {

            @Override
            public void configure(Configuration configuration) {
                Connection conn = null;
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                Config apolloConfig = ConfigService.getConfig("hbase");
                config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));
                config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));
                config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
                config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
                try {
                    conn = ConnectionFactory.createConnection(config);
                    //TableName tableName = TableName.valueOf("ods_owner_cloud:ods_receiptBillEntry");
                    table = (HTable) conn.getTable(TableName.valueOf(getTableName()));
                    scan = getScanner();
                    //scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("FCustomerID"));
                    //scan.addFamily(Bytes.toBytes("cf"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            protected Scan getScanner() {
                scan = new Scan();
                scan.addFamily(cf);
                return scan;
            }

            @Override
            protected String getTableName() {
                return "fruit";
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                tuple2.f0 = Bytes.toString(result.getRow());
                tuple2.f1 = Bytes.toString(result.getValue(cf,cq));
                return tuple2;
            }
        }).print();
    }
}
