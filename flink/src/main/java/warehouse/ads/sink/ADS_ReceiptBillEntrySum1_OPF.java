package warehouse.ads.sink;

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
import utils.MD5Utils;
import utils.ObjectUtil;
import utils.RowKeyUtil;
import warehouse.ads.entity.ADS_ReceiptBillEntrySum1Vo;

import java.io.IOException;
import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/13 12:42
 * @Description:
 **/
public class ADS_ReceiptBillEntrySum1_OPF implements OutputFormat<ADS_ReceiptBillEntrySum1Vo> {
    private static final Logger logger = LoggerFactory.getLogger(ADS_ReceiptBillEntrySum1_OPF.class);
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

        if (!admin.tableExists(TableName.valueOf("ads_owner_cloud:ads_receiptBillEntrySum1"))){
            try {
                admin.createNamespace(NamespaceDescriptor.create("ads_owner_cloud").build());
            } catch (NamespaceExistException e) {
                logger.info("ads_owner_cloud: 命名空间已存在！");
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] cf = {"cf"};

            createTable("ads_owner_cloud:ads_receiptBillEntrySum1","cf");
        }

        table = conn.getTable(TableName.valueOf("ads_owner_cloud:ads_receiptBillEntrySum1"));
    }

    @Override
    public void writeRecord(ADS_ReceiptBillEntrySum1Vo ads_receiptBillEntrySumVo) throws IOException {
        String receiverID = ads_receiptBillEntrySumVo.getReceiverID();
        String settlementTypeID = ads_receiptBillEntrySumVo.getSettlementTypeID();
        String moneydefineID = ads_receiptBillEntrySumVo.getMoneyDefineID();
        String businessType = ads_receiptBillEntrySumVo.getBusinessType();
        String tranDate = ads_receiptBillEntrySumVo.getTranDate().replace("-","").substring(2);
        String rowkey = RowKeyUtil.generateShortUuid8(MD5Utils.hash(receiverID+settlementTypeID+moneydefineID))+businessType+tranDate;
        Put put = new Put(Bytes.toBytes(rowkey));
        Map<String, Object> map = ObjectUtil.toMap(ads_receiptBillEntrySumVo);
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

    private void createTable(String tableName,String columnFamily) throws IOException{
        if (StringUtils.isBlank(tableName) || StringUtils.isEmpty(columnFamily)) {
            logger.info("===Parameters tableName|columnFamily should not be null,Please check!===");
            return;
        }
        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.info(tableName+": 表已存在！");
            return;
        }
        TableDescriptorBuilder tdesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        tdesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build());
        admin.createTable(tdesc.build());//创建表
        logger.info("create table:{} success!",tableName);

    }
}
