package warehouse.dws.sink;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ObjectUtil;
import utils.RowKeyUtil;
import warehouse.dws.entity.DWS_ReceiptBillEntrySumVo;

import java.io.IOException;
import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/13 12:42
 * @Description:
 **/
public class DWS_ReceiptBillEntrySum_OPF implements OutputFormat<DWS_ReceiptBillEntrySumVo> {
    private static final Logger logger = LoggerFactory.getLogger(DWS_ReceiptBillEntrySum_OPF.class);
    private org.apache.hadoop.conf.Configuration config = null;
    private Connection conn = null;
    private Table table = null;
    private Admin admin = null;
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
        config = HBaseConfiguration.create();
        Config apolloConfig = ConfigService.getConfig("hbase");
        config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        conn = ConnectionFactory.createConnection(config);
        admin = conn.getAdmin();

        if (!admin.tableExists(TableName.valueOf("dws_owner_cloud:dws_receiptBillEntrySum"))){
            try {
                admin.createNamespace(NamespaceDescriptor.create("dws_owner_cloud").build());
            } catch (NamespaceExistException e) {
                logger.info("dws_owner_cloud: 命名空间已存在！");
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] keys = {"1","2","3","4","5","6","7","8","9"
                    ,"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};

            byte[][] splitKeys = getSplitKeys(Arrays.asList(keys));
            String[] cf = {"cf"};
            createTableBySplitKeys("dws_owner_cloud:dws_receiptBillEntrySum",Arrays.asList(cf),splitKeys,true);
        }

        table = conn.getTable(TableName.valueOf("dws_owner_cloud:dws_receiptBillEntrySum"));
    }

    @Override
    public void writeRecord(DWS_ReceiptBillEntrySumVo dws_receiptBillEntrySumVo) throws IOException {
        Put put = new Put(Bytes.toBytes(RowKeyUtil.generateShortUuid8()));
        Map<String, Object> map = ObjectUtil.toMap(dws_receiptBillEntrySumVo);
        for (Map.Entry<String, Object> entry:map.entrySet()){
            if (entry.getValue() == null){
                logger.info("============{}.value is null=============",entry.getKey());
            }else {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }
        }
        table.put(put);
    }

    @Override
    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    private byte[][] getSplitKeys(List<String> keys) {
        byte[][] splitKeys = new byte[keys.size()][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for(String key:keys)
            rows.add(Bytes.toBytes(key));
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    private void createTableBySplitKeys(String tableName, List<String> columnFamily, byte[][] splitKeys, boolean isAsync) throws IOException {
        if (StringUtils.isBlank(tableName) || columnFamily == null
                || columnFamily.size() < 0) {
            logger.info("===Parameters tableName|columnFamily should not be null,Please check!===");
            return;
        }
        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.info(tableName+": 表已存在！");
            return;
        }
        TableDescriptorBuilder tdesc=TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for(String s: columnFamily){
            ColumnFamilyDescriptor cfd=ColumnFamilyDescriptorBuilder.of(s);
            tdesc.setColumnFamily(cfd);
        }
        TableDescriptor desc=tdesc.build();
        if (isAsync)
            admin.createTableAsync(desc,splitKeys);
        else
            admin.createTable(desc,splitKeys);
        logger.info("===Create Table " + tableName
                + " Success!columnFamily:" + columnFamily.toString()
                + "===");
    }
}
