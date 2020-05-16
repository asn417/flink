package batch.ods;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import warehouse.ods.entity.ODS_ProjectVo;
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
public class ODS_Project_IPF extends CustomTableInputFormat<ODS_ProjectVo> {

    private static final Logger logger = LoggerFactory.getLogger(ODS_Project_IPF.class);
    //结果Tuple
    ODS_ProjectVo projectVo = new ODS_ProjectVo();

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
            table = (HTable) conn.getTable(TableName.valueOf("ods_owner_cloud:ods_project"));
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
    protected ODS_ProjectVo mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        projectVo.setiD(rowKey);
        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            //2.number
            if ("FNumber".equals(column)){
                projectVo.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FName".equals(column)){//3.headID
                projectVo.setName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FExtendName".equals(column)){//3.headID
                projectVo.setExtendName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPcID".equals(column)){//3.headID
                projectVo.setPcID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPcName".equals(column)){//3.headID
                projectVo.setPcName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FInvalidDate".equals(column)){//3.headID
                projectVo.setInvalidDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreator".equals(column)){//3.headID
                projectVo.setCreator(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreateTime".equals(column)){//3.headID
                projectVo.setCreateTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                projectVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProvince".equals(column)){//3.headID
                projectVo.setProvince(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCity".equals(column)){//3.headID
                projectVo.setCity(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FArea".equals(column)){//3.headID
                projectVo.setArea(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAddress".equals(column)){//3.headID
                projectVo.setAddress(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDeveloper".equals(column)){//3.headID
                projectVo.setDeveloper(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCFolderID".equals(column)){//3.headID
                projectVo.setcFolderID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FOrgID".equals(column)){//3.headID
                projectVo.setOrgID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FImageUrl".equals(column)){//3.headID
                projectVo.setImageUrl(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAgtCode".equals(column)){//3.headID
                projectVo.setAgtCode(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDepartID".equals(column)){//3.headID
                projectVo.setDepartID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDepartType".equals(column)){//3.headID
                projectVo.setDepartType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLatitude".equals(column)){//3.headID
                projectVo.setLatitude(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLongitude".equals(column)){//3.headID
                projectVo.setLongitude(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTryBegindate".equals(column)){//3.headID
                projectVo.setTryBegindate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTryEnddate".equals(column)){//3.headID
                projectVo.setTryEnddate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBegindate".equals(column)){//3.headID
                projectVo.setBegindate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEnddate".equals(column)){//3.headID
                projectVo.setEnddate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPdAgelimit".equals(column)){//3.headID
                projectVo.setPdAgelimit(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDelay".equals(column)){//3.headID
                projectVo.setIsDelay(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FStatus".equals(column)){//3.headID
                projectVo.setStatus(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDeleteTime".equals(column)){//3.headID
                projectVo.setDeleteTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDelete".equals(column)){//3.headID
                projectVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsGenerateReceive".equals(column)){//3.headID
                projectVo.setIsGenerateReceive(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FHasGroup".equals(column)){//3.headID
                projectVo.setHasGroup(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsOpenMerchant".equals(column)){//3.headID
                projectVo.setIsOpenMerchant(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCarPrefix".equals(column)){//3.headID
                projectVo.setCarPrefix(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPhone".equals(column)){//3.headID
                projectVo.setPhone(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCloudDeptID".equals(column)){//3.headID
                projectVo.setCloudDeptID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsRecordTaxRate".equals(column)){//3.headID
                projectVo.setIsRecordTaxRate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLifeMatching".equals(column)){//3.headID
                projectVo.setLifeMatching(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTransportation".equals(column)){//3.headID
                projectVo.setTransportation(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRestaurant".equals(column)){//3.headID
                projectVo.setRestaurant(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FImageIDs".equals(column)){//3.headID
                projectVo.setImageIDs(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectProperty".equals(column)){//3.headID
                projectVo.setProjectProperty(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDepartmentID".equals(column)){//3.headID
                projectVo.setDepartmentID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsSingleBuild".equals(column)){//3.headID
                projectVo.setIsSingleBuild(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLinkPhone".equals(column)){//3.headID
                projectVo.setLinkPhone(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLinkPeople".equals(column)){//3.headID
                projectVo.setLinkPeople(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FAreaID".equals(column)){//3.headID
                projectVo.setAreaID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FWxQRCodeID".equals(column)){//3.headID
                projectVo.setWxQRCodeID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEasID".equals(column)){//3.headID
                projectVo.setEasID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEasName".equals(column)){//3.headID
                projectVo.setEasName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FZone".equals(column)){//3.headID
                projectVo.setZone(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceID".equals(column)){//3.headID
                projectVo.setSourceID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceSystem".equals(column)){//3.headID
                projectVo.setSourceSystem(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FMchNo".equals(column)){//3.headID
                projectVo.setMchNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FMchName".equals(column)){//3.headID
                projectVo.setMchName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FMchUser".equals(column)){//3.headID
                projectVo.setMchUser(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FMchPass".equals(column)){//3.headID
                projectVo.setMchPass(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFileIDs".equals(column)){//3.headID
                projectVo.setFileIDs(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLanType".equals(column)){//3.headID
                projectVo.setLanType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProduct".equals(column)){//3.headID
                projectVo.setProduct(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDescription".equals(column)){//3.headID
                projectVo.setDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return projectVo;
    }


    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "ods_owner_cloud:ods_project";
    }

}
