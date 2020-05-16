package warehouse.dws.source;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import warehouse.dws.entity.DWS_ReceiptBillEntrySumVo;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @Author: wangsen
 * @Date: 2020/5/12 16:38
 * @Description:
 **/
public class DWS_ReceiptBillEntry_IPF extends CustomTableInputFormat<DWD_ReceiptBillEntryVo> {

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
            TableName tableName = TableName.valueOf("dwd_owner_cloud:dwd_receiptBillEntry");
            table = (HTable) conn.getTable(tableName);
            scan = new Scan();
            //scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("FCustomerID"));
            //scan.addFamily(Bytes.toBytes("cf"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected DWD_ReceiptBillEntryVo mapResultToTuple(Result r) {
        DWD_ReceiptBillEntryVo receiptBillEntryVo = new DWD_ReceiptBillEntryVo();

        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));

            if ("projectID".equals(column)){
                receiptBillEntryVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("projectName".equals(column)){//3.headID
                receiptBillEntryVo.setProjectName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("receiverID".equals(column)){//3.headID
                receiptBillEntryVo.setReceiverID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("receiverName".equals(column)){//3.headID
                receiptBillEntryVo.setReceiverName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("businessType".equals(column)){//3.headID
                receiptBillEntryVo.setBusinessType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("tranDate".equals(column)){//3.headID
                receiptBillEntryVo.setTranDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("moneyDefineID".equals(column)){//3.headID
                receiptBillEntryVo.setMoneyDefineID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("moneyDefineName".equals(column)){//3.headID
                receiptBillEntryVo.setMoneyDefineName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("settlementTypeID".equals(column)){//3.headID
                receiptBillEntryVo.setSettlementTypeID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("settlementTypeName".equals(column)){//3.headID
                receiptBillEntryVo.setSettlementTypeName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("roomProperty".equals(column)){//3.headID
                receiptBillEntryVo.setRoomProperty(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("revAmount".equals(column)){//3.headID
                receiptBillEntryVo.setRevAmount(new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))));
            }else if ("revPenaltyAmount".equals(column)){//3.headID
                receiptBillEntryVo.setRevPenaltyAmount(new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))));
            }
        }
        return receiptBillEntryVo;
    }

    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "dwd_owner_cloud:dwd_receiptBillEntry";
    }
}
