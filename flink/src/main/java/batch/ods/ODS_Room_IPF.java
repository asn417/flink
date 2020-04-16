package batch.ods;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import entity.ods.ODS_RoomVo;
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
public class ODS_Room_IPF extends CustomTableInputFormat<ODS_RoomVo> {

    private static final Logger logger = LoggerFactory.getLogger(ODS_Room_IPF.class);
    //结果Tuple
    ODS_RoomVo roomVo = new ODS_RoomVo();

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
            table = (HTable) conn.getTable(TableName.valueOf("ods_owner_cloud:ods_room"));
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
    protected ODS_RoomVo mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        roomVo.setiD(rowKey);
        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            //2.number
            if ("FNumber".equals(column)){
                roomVo.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FName".equals(column)){//3.headID
                roomVo.setName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDisplayName".equals(column)){//3.headID
                roomVo.setDisplayName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectID".equals(column)){//3.headID
                roomVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildingID".equals(column)){//3.headID
                roomVo.setBuildingID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildingName".equals(column)){//3.headID
                roomVo.setBuildingName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildUnitID".equals(column)){//3.headID
                roomVo.setBuildUnitID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildUnitName".equals(column)){//3.headID
                roomVo.setBuildUnitName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFloor".equals(column)){//3.headID
                roomVo.setFloor(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSub".equals(column)){//3.headID
                roomVo.setSub(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProductTypeID".equals(column)){//3.headID
                roomVo.setProductTypeID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRoomModelID".equals(column)){//3.headID
                roomVo.setRoomModelID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRoomModelGroupID".equals(column)){//3.headID
                roomVo.setRoomModelGroupID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildingArea".equals(column)){//3.headID
                roomVo.setBuildingArea(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRoomArea".equals(column)){//3.headID
                roomVo.setRoomArea(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildingPrice".equals(column)){//3.headID
                roomVo.setBuildingPrice(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRoomPrice".equals(column)){//3.headID
                roomVo.setRoomPrice(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FStandardTotalAmount".equals(column)){//3.headID
                roomVo.setStandardTotalAmount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDealTotalAmount".equals(column)){//3.headID
                roomVo.setDealTotalAmount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBuildingPropertyID".equals(column)){//3.headID
                roomVo.setBuildingPropertyID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FHouseProperty".equals(column)){//3.headID
                roomVo.setHouseProperty(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                roomVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FChangeStatus".equals(column)){//3.headID
                roomVo.setChangeStatus(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FChangeRoomID".equals(column)){//3.headID
                roomVo.setChangeRoomID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCustomerID".equals(column)){//3.headID
                roomVo.setCustomerID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCustomerJoinInDate".equals(column)){//3.headID
                roomVo.setCustomerJoinInDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRenterID".equals(column)){//3.headID
                roomVo.setRenterID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRenterJoinInDate".equals(column)){//3.headID
                roomVo.setRenterJoinInDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDeleteTime".equals(column)){//3.headID
                roomVo.setDeleteTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDelete".equals(column)){//3.headID
                roomVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FOldPdName".equals(column)){//3.headID
                roomVo.setOldPdName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProperty".equals(column)){//3.headID
                roomVo.setProperty(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFloorID".equals(column)){//3.headID
                roomVo.setFloorID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FStatus".equals(column)){//3.headID
                roomVo.setStatus(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDescription".equals(column)){//3.headID
                roomVo.setDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFeeStatus".equals(column)){//3.headID
                roomVo.setFeeStatus(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCatalog".equals(column)){//3.headID
                roomVo.setCatalog(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCustomerName".equals(column)){//3.headID
                roomVo.setCustomerName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreateTime".equals(column)){//3.headID
                roomVo.setCreateTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FImageIDs".equals(column)){//3.headID
                roomVo.setImageIDs(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBelongToRoom".equals(column)){//3.headID
                roomVo.setBelongToRoom(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRentFlag".equals(column)){//3.headID
                roomVo.setRentFlag(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPropertyRight".equals(column)){//3.headID
                roomVo.setPropertyRight(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPurpose".equals(column)){//3.headID
                roomVo.setPurpose(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRentType".equals(column)){//3.headID
                roomVo.setRentType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSelfRenterID".equals(column)){//3.headID
                roomVo.setSelfRenterID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FHasGovPrice".equals(column)){//3.headID
                roomVo.setHasGovPrice(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFloorAliasName".equals(column)){//3.headID
                roomVo.setFloorAliasName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAuditStatus".equals(column)){//3.headID
                roomVo.setAuditStatus(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAuditorID".equals(column)){//3.headID
                roomVo.setAuditorID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAuditTime".equals(column)){//3.headID
                roomVo.setAuditTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("ffileurl".equals(column)){//3.headID
                roomVo.setFileurl(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEasID".equals(column)){//3.headID
                roomVo.setEasID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEndTime".equals(column)){//3.headID
                roomVo.setEndTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSource".equals(column)){//3.headID
                roomVo.setSource(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCompletionDate".equals(column)){//3.headID
                roomVo.setCompletionDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FGardenArea".equals(column)){//3.headID
                roomVo.setGardenArea(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLoftArea".equals(column)){//3.headID
                roomVo.setLoftArea(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReceiptAccountInfo".equals(column)){//3.headID
                roomVo.setReceiptAccountInfo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsShowApp".equals(column)){//3.headID
                roomVo.setIsShowApp(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceSystem".equals(column)){//3.headID
                roomVo.setSourceSystem(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceID".equals(column)){//3.headID
                roomVo.setSourceID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FUpdateTime".equals(column)){//3.headID
                roomVo.setUpdateTime(Bytes.toString(CellUtil.cloneValue(cell)));
            } else if ("FCreator".equals(column)){//3.headID
                roomVo.setCreator(Bytes.toString(CellUtil.cloneValue(cell)));
            } else if ("FUpdater".equals(column)){//3.headID
                roomVo.setUpdater(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return roomVo;
    }


    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "ods_owner_cloud:ods_room";
    }

}
