package warehouse.ads.source;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import warehouse.dws.entity.DWS_ReceiptBillEntrySumVo;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @Author: wangsen
 * @Date: 2020/5/12 16:38
 * @Description:
 **/
public class ADS_ReceiptBillEntrySum2_IPF extends CustomTableInputFormat<DWS_ReceiptBillEntrySumVo> {

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
            TableName tableName = TableName.valueOf("dws_owner_cloud:dws_receiptBillEntrySum");
            table = (HTable) conn.getTable(tableName);
            scan = new Scan();
            //scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("FCustomerID"));
            //scan.addFamily(Bytes.toBytes("cf"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected DWS_ReceiptBillEntrySumVo mapResultToTuple(Result r) {
        DWS_ReceiptBillEntrySumVo receiptBillEntrySumVo = new DWS_ReceiptBillEntrySumVo();

        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));

            if ("projectID".equals(column)){
                receiptBillEntrySumVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("businessType".equals(column)){//3.headID
                receiptBillEntrySumVo.setBusinessType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("tranDate".equals(column)){//3.headID
                receiptBillEntrySumVo.setTranDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("moneyDefineID".equals(column)){//3.headID
                receiptBillEntrySumVo.setMoneyDefineID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("settlementTypeID".equals(column)){//3.headID
                receiptBillEntrySumVo.setSettlementTypeID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("roomProperty".equals(column)){//3.headID
                receiptBillEntrySumVo.setRoomProperty(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("totalRevAmount".equals(column)){
                receiptBillEntrySumVo.setTotalRevAmount(new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))));
            }else if ("totalRevPenaltyAmount".equals(column)){
                receiptBillEntrySumVo.setTotalRevPenaltyAmount(new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))));
            }
        }
        return receiptBillEntrySumVo;
    }

    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "dws_owner_cloud:dws_receiptBillEntrySum";
    }
}
