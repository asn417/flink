package table.hbase;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import warehouse.ods.entity.ODS_ReceiptBillEntryVo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 13:36
 * @Description:
 **/
public class ReceiptBillEntrySource extends RichSourceFunction<ODS_ReceiptBillEntryVo> {

    private Connection conn = null;

    private Table table = null;

    private String tableName = null;
    public ReceiptBillEntrySource(String tableName){
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

        TableName tableName = TableName.valueOf("ods_owner_cloud:ods_receiptBillEntry");

        table = conn.getTable(tableName);
    }

    @Override
    public void run(SourceContext<ODS_ReceiptBillEntryVo> sourceContext) throws Exception {
        Scan scan = new Scan();
        scan.setCaching(1000);
        ResultScanner scanner = table.getScanner(scan);
        ODS_ReceiptBillEntryVo receiptBillEntryVo = null;

        for (Result result : scanner) {
            receiptBillEntryVo = new ODS_ReceiptBillEntryVo();
            //1.id
            receiptBillEntryVo.setiD(Bytes.toString(result.getRow()));
            for (Cell cell : result.listCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                //2.number
                if ("FNumber".equals(column)){
                    receiptBillEntryVo.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FHeadID".equals(column)){//3.headID
                    receiptBillEntryVo.setHeadID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FMoneyDefineID".equals(column)){//3.headID
                    receiptBillEntryVo.setMoneyDefineID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FMoneyStandardID".equals(column)){//3.headID
                    receiptBillEntryVo.setMoneyStandardID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FMoneyType".equals(column)){//3.headID
                    receiptBillEntryVo.setMoneyType(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FPeriod".equals(column)){//3.headID
                    receiptBillEntryVo.setPeriod(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceivableDate".equals(column)){//3.headID
                    receiptBillEntryVo.setReceivableDate(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceivableAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setReceivableAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FRevPenaltyAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setRevPenaltyAmount(Bytes.toBigDecimal(CellUtil.cloneValue(cell)));
                }else if ("FRevAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setRevAmount(Bytes.toBigDecimal(CellUtil.cloneValue(cell)));
                }else if ("FDescription".equals(column)){//3.headID
                    receiptBillEntryVo.setDescription(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FBusinessType".equals(column)){//3.headID
                    receiptBillEntryVo.setBusinessType(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FStatus".equals(column)){//3.headID
                    receiptBillEntryVo.setStatus(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FCreator".equals(column)){//3.headID
                    receiptBillEntryVo.setCreator(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FCreateTime".equals(column)){//3.headID
                    receiptBillEntryVo.setCreateTime(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FECID".equals(column)){//3.headID
                    receiptBillEntryVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceiveID".equals(column)){//3.headID
                    receiptBillEntryVo.setReceiveID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FQty".equals(column)){//3.headID
                    receiptBillEntryVo.setQty(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FPrice".equals(column)){//3.headID
                    receiptBillEntryVo.setPrice(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FDeleteTime".equals(column)){//3.headID
                    receiptBillEntryVo.setDeleteTime(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FIsDelete".equals(column)){//3.headID
                    receiptBillEntryVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FTaxRate".equals(column)){//3.headID
                    receiptBillEntryVo.setTaxRate(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FTaxAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setTaxAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FIncomeAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setIncomeAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FOffSetMoneydefineID".equals(column)){//3.headID
                    receiptBillEntryVo.setOffSetMoneydefineID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FMonth".equals(column)){//3.headID
                    receiptBillEntryVo.setMonth(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FRatio".equals(column)){//3.headID
                    receiptBillEntryVo.setRatio(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FLadderPriceAndValues".equals(column)){//3.headID
                    receiptBillEntryVo.setLadderPriceAndValues(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FInvoicedAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setInvoicedAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FInvoicedID".equals(column)){//3.headID
                    receiptBillEntryVo.setInvoicedID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FInvoicedType".equals(column)){//3.headID
                    receiptBillEntryVo.setInvoicedType(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FDataSourceType".equals(column)){//3.headID
                    receiptBillEntryVo.setDataSourceType(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FIsVoucher".equals(column)){//3.headID
                    receiptBillEntryVo.setIsVoucher(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FVoucherID".equals(column)){//3.headID
                    receiptBillEntryVo.setVoucherID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceivePeriod".equals(column)){//3.headID
                    receiptBillEntryVo.setReceivePeriod(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FSettlementTypeID".equals(column)){//3.headID
                    receiptBillEntryVo.setSettlementTypeID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReFundAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setReFundAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceiveDate".equals(column)){//3.headID
                    receiptBillEntryVo.setReceiveDate(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FLastDosage".equals(column)){//3.headID
                    receiptBillEntryVo.setLastDosage(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FCurrentDosage".equals(column)){//3.headID
                    receiptBillEntryVo.setCurrentDosage(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FRoomID".equals(column)){//3.headID
                    receiptBillEntryVo.setRoomID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceiveStartDate".equals(column)){//3.headID
                    receiptBillEntryVo.setReceiveStartDate(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceiveEndDate".equals(column)){//3.headID
                    receiptBillEntryVo.setReceiveEndDate(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReduPenaltyAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setReduPenaltyAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FParentEntryID".equals(column)){//3.headID
                    receiptBillEntryVo.setParentEntryID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FRange".equals(column)){//3.headID
                    receiptBillEntryVo.setRange(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FInsEntryID".equals(column)){//3.headID
                    receiptBillEntryVo.setInsEntryID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FArPenaltyAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setArPenaltyAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FStoreAmount".equals(column)){//3.headID
                    receiptBillEntryVo.setStoreAmount(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FStoreBalance".equals(column)){//3.headID
                    receiptBillEntryVo.setStoreBalance(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FProjectID".equals(column)){//3.headID
                    receiptBillEntryVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FBalanceID".equals(column)){//3.headID
                    receiptBillEntryVo.setBalanceID(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FIsLock".equals(column)){//3.headID
                    receiptBillEntryVo.setIsLock(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FBillNo".equals(column)){//3.headID
                    receiptBillEntryVo.setBillNo(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FRemark".equals(column)){//3.headID
                    receiptBillEntryVo.setRemark(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FIsReducePenalty".equals(column)){//3.headID
                    receiptBillEntryVo.setIsReducePenalty(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FReceiveCreator".equals(column)){//3.headID
                    receiptBillEntryVo.setReceiveCreator(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FFirstStage".equals(column)){//3.headID
                    receiptBillEntryVo.setFirstStage(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FIsInvoiced".equals(column)){//3.headID
                    receiptBillEntryVo.setIsInvoiced(Bytes.toString(CellUtil.cloneValue(cell)));
                }else if ("FUpdateTime".equals(column)){//3.headID
                    receiptBillEntryVo.setUpdateTime(Bytes.toString(CellUtil.cloneValue(cell)));
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
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
