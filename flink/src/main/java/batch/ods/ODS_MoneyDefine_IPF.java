package batch.ods;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import entity.ods.ODS_MoneyDefineVo;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 16:34
 * @Description:
 **/
public class ODS_MoneyDefine_IPF extends CustomTableInputFormat<ODS_MoneyDefineVo> {

    private static final Logger logger = LoggerFactory.getLogger(ODS_MoneyDefine_IPF.class);
    //结果Tuple
    ODS_MoneyDefineVo moneyDefineVo = new ODS_MoneyDefineVo();

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
            table = (HTable) conn.getTable(TableName.valueOf("ods_owner_cloud:ods_moneyDefine"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        scan = new Scan();
    }

    /***
     * @Author: wangsen
     * @Description: 处理获取的数据
     * @Date: 2020/4/14
     * @Param: [r]
     * @Return: org.apache.flink.api.java.tuple.Tuple2<java.lang.String,java.lang.String>
     **/
    @Override
    protected ODS_MoneyDefineVo mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        moneyDefineVo.setiD(rowKey);
        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            //2.number
            if ("FNumber".equals(column)){
                moneyDefineVo.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FName".equals(column)){//3.headID
                moneyDefineVo.setName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEnName".equals(column)){//3.headID
                moneyDefineVo.setEnName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FInsID".equals(column)){//3.headID
                moneyDefineVo.setInsID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDescription".equals(column)){//3.headID
                moneyDefineVo.setDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsSys".equals(column)){//3.headID
                moneyDefineVo.setIsSys(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsEnabled".equals(column)){//3.headID
                moneyDefineVo.setIsEnabled(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FType".equals(column)){//3.headID
                moneyDefineVo.setType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                moneyDefineVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectID".equals(column)){//3.headID
                moneyDefineVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FType".equals(column)){//3.headID
                moneyDefineVo.setType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBillingCycle".equals(column)){//3.headID
                moneyDefineVo.setBillingCycle(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCycleType".equals(column)){//3.headID
                moneyDefineVo.setCycleType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCycleStartVal".equals(column)){//3.headID
                moneyDefineVo.setCycleStartVal(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCycleEndVal".equals(column)){//3.headID
                moneyDefineVo.setCycleEndVal(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReserved".equals(column)){//3.headID
                moneyDefineVo.setReserved(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReservedType".equals(column)){//3.headID
                moneyDefineVo.setReservedType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAccRecType".equals(column)){//3.headID
                moneyDefineVo.setAccRecType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAccRecDate".equals(column)){//3.headID
                moneyDefineVo.setAccRecDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FArDateSetType".equals(column)){//3.headID
                moneyDefineVo.setArDateSetType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsComBreach".equals(column)){//3.headID
                moneyDefineVo.setIsComBreach(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreator".equals(column)){//3.headID
                moneyDefineVo.setCreator(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreateTime".equals(column)){//3.headID
                moneyDefineVo.setCreateTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                moneyDefineVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectID".equals(column)){//3.headID
                moneyDefineVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FGroupID".equals(column)){//3.headID
                moneyDefineVo.setGroupID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FMoneyType".equals(column)){//3.headID
                moneyDefineVo.setMoneyType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPreMoneyID".equals(column)){//3.headID
                moneyDefineVo.setPreMoneyID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDeleteTime".equals(column)){//3.headID
                moneyDefineVo.setDeleteTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDelete".equals(column)){//3.headID
                moneyDefineVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTaxRate".equals(column)){//3.headID
                moneyDefineVo.setTaxRate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsRemit".equals(column)){//3.headID
                moneyDefineVo.setIsRemit(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAccountNumber".equals(column)){//3.headID
                moneyDefineVo.setAccountNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsLayerFee".equals(column)){//3.headID
                moneyDefineVo.setIsLayerFee(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FYearType".equals(column)){//3.headID
                moneyDefineVo.setYearType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FUpdateTime".equals(column)){//3.headID
                moneyDefineVo.setUpdateTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FOrgID".equals(column)){//3.headID
                moneyDefineVo.setOrgID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCleanType".equals(column)){//3.headID
                moneyDefineVo.setCleanType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FChargeDimension".equals(column)){//3.headID
                moneyDefineVo.setChargeDimension(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCustomType".equals(column)){//3.headID
                moneyDefineVo.setCustomType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCustomCycle".equals(column)){//3.headID
                moneyDefineVo.setCustomCycle(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FExpenseItemNo".equals(column)){//3.headID
                moneyDefineVo.setExpenseItemNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FExpenseItemName".equals(column)){//3.headID
                moneyDefineVo.setExpenseItemName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTaxRateEas".equals(column)){//3.headID
                moneyDefineVo.setTaxRateEas(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FMeasureUnitCore".equals(column)){//3.headID
                moneyDefineVo.setMeasureUnitCore(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProductNameNo".equals(column)){//3.headID
                moneyDefineVo.setProductNameNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProductName".equals(column)){//3.headID
                moneyDefineVo.setProductName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFeetypeNo".equals(column)){//3.headID
                moneyDefineVo.setFeetypeNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFeetypeName".equals(column)){//3.headID
                moneyDefineVo.setFeetypeName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRectypeNo".equals(column)){//3.headID
                moneyDefineVo.setRectypeNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRectypeName".equals(column)){//3.headID
                moneyDefineVo.setRectypeName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEasbusiness".equals(column)){//3.headID
                moneyDefineVo.setEasbusiness(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEasfinance".equals(column)){//3.headID
                moneyDefineVo.setEasfinance(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FUnitPrice".equals(column)){//3.headID
                moneyDefineVo.setUnitPrice(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FChargeType".equals(column)){//3.headID
                moneyDefineVo.setChargeType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReceivePeriodType".equals(column)){//3.headID
                moneyDefineVo.setReceivePeriodType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FInvoicedItemNameType".equals(column)){//3.headID
                moneyDefineVo.setInvoicedItemNameType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FInvoicedItemName".equals(column)){//3.headID
                moneyDefineVo.setInvoicedItemName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceSystem".equals(column)){//3.headID
                moneyDefineVo.setSourceSystem(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceID".equals(column)){//3.headID
                moneyDefineVo.setSourceID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FisCreateReceive".equals(column)){//3.headID
                moneyDefineVo.setIsCreateReceive(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAmountCalType".equals(column)){//3.headID
                moneyDefineVo.setAmountCalType(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return moneyDefineVo;
    }

    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "ods_owner_cloud:ods_moneyDefine";
    }

}
