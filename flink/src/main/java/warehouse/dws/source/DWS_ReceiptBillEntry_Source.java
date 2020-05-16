package warehouse.dws.source;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;

import java.util.Iterator;

/**
 * @Author: wangsen
 * @Date: 2020/5/13 15:45
 * @Description:
 **/
public class DWS_ReceiptBillEntry_Source extends RichSourceFunction<DWD_ReceiptBillEntryVo> {

    private Connection conn = null;

    private Table table = null;

    private Scan scan = null;

    /**
     * 在open方法中使用hbase的客户端连接
     *
     * @param
     * @throws Exception
     */

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        Config apolloConfig = ConfigService.getConfig("hbase");
        config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        conn = ConnectionFactory.createConnection(config);

        TableName tableName = TableName.valueOf("dwd_owner_cloud:dwd_receiptBillEntry");

        table = conn.getTable(tableName);

        scan = new Scan();

    }


    /**
     * run方法来自java接口文件sourceFunction
     *
     * @param sourceContext
     * @throws Exception
     */

    @Override
    public void run(SourceContext<DWD_ReceiptBillEntryVo> sourceContext) throws Exception {

        Iterator<Result> iterator = table.getScanner(scan).iterator();

        while (iterator.hasNext()) {
            DWD_ReceiptBillEntryVo receiptBillEntryVo = new DWD_ReceiptBillEntryVo();
            Result next = iterator.next();
            for (Cell cell:next.listCells()){
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
                    receiptBillEntryVo.setRevAmount(Bytes.toBigDecimal(CellUtil.cloneValue(cell)));
                }else if ("revPenaltyAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setRevPenaltyAmount(Bytes.toBigDecimal(CellUtil.cloneValue(cell)));
                }
            }

            sourceContext.collect(receiptBillEntryVo);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
