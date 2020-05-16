package table.hbase;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import warehouse.ods.entity.ODS_ReceiptBillVo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 13:36
 * @Description:
 **/
public class ReceiptBillSource extends RichSourceFunction<ODS_ReceiptBillVo> {

    private Connection conn = null;

    private Table table = null;

    private Scan scan = null;
    private Admin admin = null;

    private String tableName = null;
    public ReceiptBillSource(String tableName){
        this.tableName = tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        Config apolloConfig = ConfigService.getConfig("hbase");
        //打开hbase连接
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));

        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));

        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 3000);

        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 3000);

        conn = ConnectionFactory.createConnection(config);
        admin = conn.getAdmin();
        //tableName = "ods_owner_cloud:ods_receiptBill";
        if (!admin.tableExists(TableName.valueOf(tableName))){
            HBaseUtils.createNamespace("ods_owner_cloud");
            String[] keys = {"1","2","3","4","5","6","7","8","9"};
            byte[][] splitKeys = HBaseUtils.getSplitKeys(Arrays.asList(keys));
            String[] cf = {"cf"};
            HBaseUtils.createTableBySplitKeys(tableName,Arrays.asList(cf),splitKeys,true);
        }
        TableName tableName = TableName.valueOf("ods_owner_cloud:ods_receiptBill");

        conn = ConnectionFactory.createConnection(config);

        table = conn.getTable(tableName);
    }

    @Override
    public void run(SourceContext<ODS_ReceiptBillVo> sourceContext) throws Exception {
        Scan scan = new Scan();
        scan.setCaching(1000);
        ResultScanner scanner = table.getScanner(scan);
        ODS_ReceiptBillVo receiptBillVo = null;
        for (Result result : scanner) {
            receiptBillVo = new ODS_ReceiptBillVo();
            receiptBillVo.setiD(Bytes.toString(result.getRow()));
            for (Cell cell : result.listCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                if ("FProjectID".equals(column)){
                    receiptBillVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FRoomID".equals(column)){
                    receiptBillVo.setRoomID(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            sourceContext.collect(receiptBillVo);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
