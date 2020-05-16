package batch.ods;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import warehouse.ods.entity.ODS_SettlementTypeVo;
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
public class ODS_SettlementType_IPF extends CustomTableInputFormat<ODS_SettlementTypeVo> {

    private static final Logger logger = LoggerFactory.getLogger(ODS_SettlementType_IPF.class);
    //结果Tuple
    ODS_SettlementTypeVo settlementTypeVo = new ODS_SettlementTypeVo();

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
            table = (HTable) conn.getTable(TableName.valueOf("ods_owner_cloud:ods_settlementType"));
            scan = new Scan();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * @Author: wangsen
     * @Description: 处理获取的数据
     * @Date: 2020/4/14
     * @Param: [r]
     * @Return: org.apache.flink.api.java.tuple.Tuple2<java.lang.String,java.lang.String>
     **/
    @Override
    protected ODS_SettlementTypeVo mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        settlementTypeVo.setiD(rowKey);
        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            //2.number
            if ("FNumber".equals(column)){
                settlementTypeVo.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FName".equals(column)){//3.headID
                settlementTypeVo.setName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBankID".equals(column)){//3.headID
                settlementTypeVo.setBankID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAccount".equals(column)){//3.headID
                settlementTypeVo.setAccount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsSys".equals(column)){//3.headID
                settlementTypeVo.setIsSys(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsEnabled".equals(column)){//3.headID
                settlementTypeVo.setIsEnabled(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDefault".equals(column)){//3.headID
                settlementTypeVo.setIsDefault(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDescription".equals(column)){//3.headID
                settlementTypeVo.setDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                settlementTypeVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectID".equals(column)){//3.headID
                settlementTypeVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FType".equals(column)){//3.headID
                settlementTypeVo.setType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDeleteTime".equals(column)){//3.headID
                settlementTypeVo.setDeleteTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDelete".equals(column)){//3.headID
                settlementTypeVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFinancialStructureNumber".equals(column)){//3.headID
                settlementTypeVo.setFinancialStructureNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBankAccountNumber".equals(column)){//3.headID
                settlementTypeVo.setBankAccountNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FScanningGunType".equals(column)){//3.headID
                settlementTypeVo.setScanningGunType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBankAccountID".equals(column)){//3.headID
                settlementTypeVo.setBankAccountID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBankBaseDataID".equals(column)){//3.headID
                settlementTypeVo.setBankBaseDataID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPayStyle".equals(column)){//3.headID
                settlementTypeVo.setPayStyle(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPayType".equals(column)){//3.headID
                settlementTypeVo.setPayType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FOldID".equals(column)){//3.headID
                settlementTypeVo.setOldID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBankAccount".equals(column)){//3.headID
                settlementTypeVo.setBankAccount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceSystem".equals(column)){//3.headID
                settlementTypeVo.setSourceSystem(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceID".equals(column)){//3.headID
                settlementTypeVo.setSourceID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsShow".equals(column)){//3.headID
                settlementTypeVo.setIsShow(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBusinessType".equals(column)){//3.headID
                settlementTypeVo.setBusinessType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FThirdPayId".equals(column)){//3.headID
                settlementTypeVo.setThirdPayId(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTradeType".equals(column)){//3.headID
                settlementTypeVo.setTradeType(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return settlementTypeVo;
    }


    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "ods_owner_cloud:ods_settlementType";
    }

}
