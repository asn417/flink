package warehouse.dws.sink;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
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
 * @Date: 2020/5/13 10:10
 * @Description:
 **/
public class DWS_ReceiptBillEntrySum_Sink extends RichSinkFunction<DWS_ReceiptBillEntrySumVo> {

    private static final Logger logger = LoggerFactory.getLogger(DWS_ReceiptBillEntrySum_Sink.class);

    private Connection conn = null;
    private Table table = null;
    private Admin admin = null;


    private static String zkServer;
    private static String zkPort;
    private static TableName tableName;

    private static final String cf = "cf";
    BufferedMutatorParams params;
    BufferedMutator mutator;

    @Override
    public void open(Configuration parameters) throws Exception {
        /*ParameterTool para = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        zkServer = para.getRequired("hbase.zkServer");
        zkPort = para.getRequired("hbase.zkPort");
        String tName = para.getRequired("hbase.tableName");
        tableName = TableName.valueOf(tName);*/

        zkServer = "172.20.184.17";
        zkPort = "2181";
        tableName = TableName.valueOf("dws_owner_cloud:dws_receiptBillEntrySum");

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", zkServer);
        config.set("hbase.zookeeper.property.clientPort", zkPort);

        conn = ConnectionFactory.createConnection(config);
        admin = conn.getAdmin();

        if (!admin.tableExists(tableName)){
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

        // 设置缓存
        params = new BufferedMutatorParams(tableName);
        params.writeBufferSize(1024);
        mutator = conn.getBufferedMutator(params);
    }

    @Override
    public void invoke(DWS_ReceiptBillEntrySumVo record, Context context) throws Exception {
        Put put = new Put(Bytes.toBytes(RowKeyUtil.generateShortUuid8()));
        Map<String, Object> map = ObjectUtil.toMap(record);
        for (Map.Entry<String, Object> entry:map.entrySet()){
            if (!"iD".equals(entry.getKey()) && entry.getValue() != null){
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(entry.getValue().toString()));
            }
        }
        //table.put(put);
        mutator.mutate(put);
    }

    @Override
    public void close() throws Exception {
        mutator.flush();
        conn.close();
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
