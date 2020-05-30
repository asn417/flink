package Flink;

import com.alibaba.otter.canal.protocol.Message;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import kafka.MyCustomDeserializationSchema;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.hbase.HBaseConfig;
import utils.HBaseUtil;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/29 9:59
 * @Description: flink整合apollo、elasticsearch、hbase、mybatis
 **/
public class FlinkReadCanal {
    private static Logger logger = LoggerFactory.getLogger(FlinkReadCanal.class);
    //初始化es客户端
    private static ClientInterface clientByXML = ElasticSearchHelper.getConfigRestClientUtil("esmapper/chargeReport.xml");

    //从apollo获取hbase配置
    private static HBaseConfig hBaseConfig;

    private static String tableName = "transferBill1";
    private static String requestHead = "hkey_transferbill/data/_search";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //1、设置statebackend
        environment.setStateBackend(new RocksDBStateBackend("hdfs://master:9000/transferBill/checkpoints",true));
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
        checkpointConfig.setCheckpointInterval(1000);
        // 设置模式为exactly-once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在6s内完成，或者被丢弃【checkpoint的超时时间】
        checkpointConfig.setCheckpointTimeout(6000);
        // 同一时间只允许进行一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        environment.getConfig().registerTypeWithKryoSerializer(Message.class, DefaultSerializers.StringSerializer.class);
        //2、配置kafka消费者
        Config kafka_config = ConfigService.getConfig("bigData.kafka");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.getProperty("bootstrap.servers","172.20.184.17:9092"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"canal_binlog_group");
        FlinkKafkaConsumer<Message> consumer = new FlinkKafkaConsumer<>(kafka_config.getProperty("kafka.topic", "canal_binlog"), new MyCustomDeserializationSchema(), properties);
        consumer.setStartFromGroupOffsets();//如果不从checkpoint重启，则使用此配置
        SingleOutputStreamOperator<Tuple4<String, Map<String, Object>, Map<String, Object>,String>> map = environment.addSource(consumer).map(new MyMapFunction());

        SingleOutputStreamOperator<Tuple4<String, Map<String, Object>, Map<String, Object>,String>> filter = map.filter(new FilterFunction<Tuple4<String, Map<String, Object>, Map<String, Object>,String>>() {
            @Override
            public boolean filter(Tuple4<String, Map<String, Object>, Map<String, Object>,String> value) throws Exception {
                return value != null;//非增删改的数据返回的为null对象
            }
        });

        filter.print();

        //分流
        SplitStream<Tuple4<String, Map<String, Object>, Map<String, Object>,String>> split = filter.split(new OutputSelector<Tuple4<String, Map<String, Object>, Map<String, Object>,String>>() {
            @Override
            public Iterable<String> select(Tuple4<String, Map<String, Object>, Map<String, Object>,String> value) {
                List<String> output = new ArrayList<>();
                if ("insert".equals(value.f0)) {
                    output.add("insert");
                }
                if ("delete".equals(value.f0)) {
                    output.add("delete");
                }
                if ("update".equals(value.f0)) {
                    output.add("update");
                }
                return output;
            }
        });
        DataStream<Tuple4<String, Map<String, Object>, Map<String, Object>,String>> insert = split.select("insert");
        DataStream<Tuple4<String, Map<String, Object>, Map<String, Object>,String>> delete = split.select("delete");
        DataStream<Tuple4<String, Map<String, Object>, Map<String, Object>,String>> update = split.select("update");
        //月份映射
        Map<String,String> monthMap = new HashMap<>();
        monthMap.put("01","Jan");
        monthMap.put("02","Feb");
        monthMap.put("03","Mar");
        monthMap.put("04","Apr");
        monthMap.put("05","May");
        monthMap.put("06","Jun");
        monthMap.put("07","Jul");
        monthMap.put("08","Aug");
        monthMap.put("09","Sep");
        monthMap.put("10","Oct");
        monthMap.put("11","Nov");
        monthMap.put("12","Dec");
        //下面分别针对增、删、改的分流数据进行逻辑处理
        //插入逻辑
        insert.map(new MapFunction<Tuple4<String, Map<String, Object>, Map<String, Object>,String>, Object>() {
            @Override
            public Object map(Tuple4<String, Map<String, Object>, Map<String, Object>,String> value) throws Exception {
                BigDecimal totalAmount = null;
                BigDecimal monthAmount = null;
                String remark = null;
                BigDecimal changeAmount = null;
                String remarkAfter = value.f2.get("FRemark").toString();
                //从es查询，判断是否已有对应的统计数据。
                Map<String,Object> paramMap = new HashMap<>();
                paramMap.put("projectID",value.f2.get("FProjectID").toString());
                paramMap.put("roomID",value.f2.get("FRoomID").toString());
                paramMap.put("customerID",value.f2.get("FCustomerID").toString());
                paramMap.put("moneyDefineID",value.f2.get("FMoneyDefineID").toString());
                paramMap.put("year",value.f2.get("FPeriod").toString().substring(0,4));
                paramMap.put("transferType",value.f2.get("FType").toString());

                ESDatas<Map> esDatas = ESUtils.getAllDataByParam(clientByXML,requestHead,paramMap,"getDataByParam",Map.class);
                List<Map> datas = esDatas.getDatas();
                //1.如果没有对应的统计数据
                if (datas == null){
                    logger.info("es中没有对应的统计数据，插入统计数据");
                    newDataInsert(clientByXML,"transferBill1",value.f2,value.f3);
                    //2.如果已有对应的统计数据
                }else if (datas.size() == 1){
                    logger.info("es中已有对应的统计数据，更新统计数据");
                    String rowkey = datas.get(0).get("rowkey").toString();
                    //判断rowkey是否为空，如果不为空则获取hbase数据，更新对应月份金额和总金额即可；如果为空，就要新生成数据，同步插入到es和hbase
                    HBaseUtil.getInstance(hBaseConfig);
                    List<Cell> cells = HBaseUtil.getDataByKey(tableName, rowkey);
                    BigDecimal revAmountAfter = new BigDecimal(value.f2.get("FRevAmount").toString()).setScale(2, BigDecimal.ROUND_HALF_UP);
                    BigDecimal revPenaltyAmountAfter = new BigDecimal(value.f2.get("FRevPenaltyAmount").toString()).setScale(2, BigDecimal.ROUND_HALF_UP);
                    //hbase中已存的金额
                    for (Cell cell:cells){
                        if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("total")){
                            totalAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                        }else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(monthMap.get(value.f2.get("FPeriod").toString().substring(5,7)))){
                            monthAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                        }else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("remark")){
                            remark = Bytes.toString(CellUtil.cloneValue(cell));
                        }
                    }
                    changeAmount = revAmountAfter.add(revPenaltyAmountAfter);
                    if (totalAmount != null){
                        HBaseUtil.putData(tableName,rowkey,"data","total",totalAmount.add(changeAmount).toString());
                    }else {
                        logger.error("数据异常，修改的数据在hbase中没有对应的total字段，或total值为null！");
                    }
                    if (monthAmount != null){
                        HBaseUtil.putData(tableName,rowkey,"data",monthMap.get(value.f2.get("FPeriod").toString().substring(5,7)),monthAmount.add(changeAmount).toString());
                    }else {
                        HBaseUtil.putData(tableName,rowkey,"data",monthMap.get(value.f2.get("FPeriod").toString().substring(5,7)),changeAmount.toString());
                    }
                    if (StringUtils.isNotEmpty(remarkAfter)){
                        HBaseUtil.putData(tableName,rowkey,"data","remark",StringUtils.isEmpty(remark)?remarkAfter:remark+","+remarkAfter);//更新备注
                    }
                    //3.数据有异常
                }else {
                    StringBuilder sb = new StringBuilder();
                    for (Map data:datas){
                        sb.append(data.get("rowkey").toString()+",");
                    }
                    logger.error("数据异常，从es获取的数据应该只有一条，实际获取的是{}条！",datas.size());
                    logger.error("rowkey为：",sb.substring(0,sb.lastIndexOf(",")));
                }
                return null;
            }
        });
        //删除逻辑
        delete.map(new MapFunction<Tuple4<String, Map<String, Object>, Map<String, Object>,String>, Object>() {
            @Override
            public Object map(Tuple4<String, Map<String, Object>, Map<String, Object>,String> value) throws Exception {
                if ("0".equals(value.f1.get("FIsCancel").toString()) && "0".equals(value.f1.get("FIsDelete").toString())) {
                    //删除的金额
                    BigDecimal revAmountBefore = new BigDecimal(value.f1.get("FRevAmount").toString()).setScale(2, BigDecimal.ROUND_HALF_UP);
                    BigDecimal revPenaltyAmountBefore = new BigDecimal(value.f1.get("FRevPenaltyAmount").toString()).setScale(2, BigDecimal.ROUND_HALF_UP);
                    String remarkBefore = value.f1.get("FRemark").toString();//备注
                    BigDecimal changeAmount = revAmountBefore.add(revPenaltyAmountBefore);
                    BigDecimal totalAmount = null;
                    BigDecimal monthAmount = null;
                    String remark = null;

                    //从es查询，判断是否已有对应的统计数据。
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("projectID", value.f1.get("FProjectID").toString());
                    paramMap.put("roomID", value.f1.get("FRoomID").toString());
                    paramMap.put("customerID", value.f1.get("FCustomerID").toString());
                    paramMap.put("moneyDefineID", value.f1.get("FMoneyDefineID").toString());
                    paramMap.put("year", value.f1.get("FPeriod").toString().substring(0, 4));
                    paramMap.put("transferType", value.f1.get("FType").toString());
                    ESDatas<Map> esDatas = ESUtils.getAllDataByParam(clientByXML, requestHead, paramMap, "getDataByParam", Map.class);
                    List<Map> datas = esDatas.getDatas();

                    if (datas == null) {
                        logger.error("数据异常，es中没有对应的统计数据");
                        //2.如果已有对应的统计数据
                    } else if (datas.size() == 1) {
                        logger.info("es中已有对应的统计数据，更新统计数据");
                        //hbase中已存的金额
                        String rowkey = datas.get(0).get("rowkey").toString();
                        HBaseUtil.getInstance(hBaseConfig);
                        List<Cell> cells = HBaseUtil.getDataByKey(tableName, rowkey);
                        for (Cell cell : cells) {
                            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("total")) {
                                totalAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)))) {
                                monthAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("remark")) {
                                remark = Bytes.toString(CellUtil.cloneValue(cell));
                            }
                        }
                        //修改所删月份的金额和总金额
                        if (totalAmount != null) {
                            HBaseUtil.putData(tableName, rowkey, "data", "total", totalAmount.subtract(changeAmount).toString());
                        } else {
                            logger.error("数据异常，修改的数据在hbase中没有对应的total字段，或total值为null！");
                        }
                        if (monthAmount != null) {
                            HBaseUtil.putData(tableName, rowkey, "data", monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)), monthAmount.subtract(changeAmount).toString());
                        } else {
                            logger.error("数据异常，修改的数据在hbase中没有对应的{}字段，或{}值为null！", monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)), monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)));
                        }
                        if (StringUtils.isNotEmpty(remarkBefore)) {
                            HBaseUtil.putData(tableName, rowkey, "data", "remark", StringUtils.isEmpty(remark) ? remarkBefore : remark + "," + remarkBefore);//更新备注
                        }
                        //3.数据有异常
                    } else {
                        StringBuilder sb = new StringBuilder();
                        for (Map data : datas) {
                            sb.append(data.get("rowkey").toString() + ",");
                        }
                        logger.error("数据异常，从es获取的数据应该只有一条，实际获取的是{}条！", datas.size());
                        logger.error("rowkey为：", sb.substring(0, sb.lastIndexOf(",")));
                    }
                }
                return null;
            }
        });

        //更新逻辑
        update.map(new MapFunction<Tuple4<String, Map<String, Object>, Map<String, Object>,String>, Object>() {
            @Override
            public Object map(Tuple4<String, Map<String, Object>, Map<String, Object>,String> value) throws Exception {
                String isCancelBefore = value.f1.get("FIsCancel").toString();
                String isCancelAfter = value.f2.get("FIsCancel").toString();
                String isDeleteBefore = value.f1.get("FIsDelete").toString();
                String isDeleteAfter = value.f2.get("FIsDelete").toString();
                String remarkAfter = value.f2.get("FRemark").toString();

                BigDecimal totalAmount = null;
                BigDecimal monthAmount = null;
                String remark = null;

                //从es查询，判断是否已有对应的统计数据。
                Map<String,Object> paramMap = new HashMap<>();
                paramMap.put("projectID",value.f1.get("FProjectID").toString());
                paramMap.put("roomID",value.f1.get("FRoomID").toString());
                paramMap.put("customerID",value.f1.get("FCustomerID").toString());
                paramMap.put("moneyDefineID",value.f1.get("FMoneyDefineID").toString());
                paramMap.put("year",value.f1.get("FPeriod").toString().substring(0,4));
                paramMap.put("transferType",value.f1.get("FType").toString());

                ESDatas<Map> esDatas = ESUtils.getAllDataByParam(clientByXML, requestHead, paramMap, "getDataByParam", Map.class);
                List<Map> datas = esDatas.getDatas();
                //逻辑删除
                if (("0".equals(isCancelBefore) && "1".equals(isCancelAfter)) || ("0".equals(isDeleteBefore) && "1".equals(isDeleteAfter))){
                    //删除的金额
                    BigDecimal revAmountBefore = new BigDecimal(value.f1.get("FRevAmount").toString()).setScale(2,BigDecimal.ROUND_HALF_UP);
                    BigDecimal revPenaltyAmountBefore = new BigDecimal(value.f1.get("FRevPenaltyAmount").toString()).setScale(2,BigDecimal.ROUND_HALF_UP);
                    String remarkBefore = value.f1.get("FRemark").toString();//备注
                    BigDecimal changeAmount = revAmountBefore.add(revPenaltyAmountBefore);

                    if (datas == null){
                        logger.error("数据异常，es中没有对应的统计数据");
                        //2.如果已有对应的统计数据
                    }else if (datas.size() == 1){
                        logger.info("es中已有对应的统计数据，更新统计数据");
                        //hbase中已存的金额
                        String rowkey = datas.get(0).get("rowkey").toString();
                        HBaseUtil.getInstance(hBaseConfig);
                        List<Cell> cells = HBaseUtil.getDataByKey(tableName, rowkey);
                        for (Cell cell:cells){
                            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("total")){
                                totalAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                            }else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(monthMap.get(value.f1.get("FPeriod").toString().substring(5,7)))){
                                monthAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                            }else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("remark")){
                                remark = Bytes.toString(CellUtil.cloneValue(cell));
                            }
                        }
                        //修改所删月份的金额和总金额
                        if (totalAmount != null){
                            HBaseUtil.putData(tableName,rowkey,"data","total",totalAmount.subtract(changeAmount).toString());
                        }else {
                            logger.error("数据异常，修改的数据在hbase中没有对应的total字段，或total值为null！");
                        }
                        if (monthAmount != null){
                            HBaseUtil.putData(tableName,rowkey,"data",monthMap.get(value.f1.get("FPeriod").toString().substring(5,7)),monthAmount.subtract(changeAmount).toString());
                        }else {
                            logger.error("数据异常，修改的数据在hbase中没有对应的{}字段，或{}值为null！",monthMap.get(value.f1.get("FPeriod").toString().substring(5,7)),monthMap.get(value.f1.get("FPeriod").toString().substring(5,7)));
                        }
                        if (StringUtils.isNotEmpty(remarkBefore)){
                            HBaseUtil.putData(tableName,rowkey,"data","remark",StringUtils.isEmpty(remark)?remarkBefore:remark+","+remarkBefore);//更新备注
                        }
                        //3.数据有异常
                    }else {
                        StringBuilder sb = new StringBuilder();
                        for (Map data:datas){
                            sb.append(data.get("rowkey").toString()+",");
                        }
                        logger.error("数据异常，从es获取的数据应该只有一条，实际获取的是{}条！",datas.size());
                        logger.error("rowkey为：",sb.substring(0,sb.lastIndexOf(",")));
                    }
                    //非逻辑删除
                }else {
                    //合计金额的变动量
                    BigDecimal revAmountBefore = new BigDecimal(value.f1.get("FRevAmount").toString()).setScale(2,BigDecimal.ROUND_HALF_UP);
                    BigDecimal revPenaltyAmountBefore = new BigDecimal(value.f1.get("FRevPenaltyAmount").toString()).setScale(2,BigDecimal.ROUND_HALF_UP);
                    BigDecimal revAmountAfter = new BigDecimal(value.f2.get("FRevAmount").toString()).setScale(2, BigDecimal.ROUND_HALF_UP);
                    BigDecimal revPenaltyAmountAfter = new BigDecimal(value.f2.get("FRevPenaltyAmount").toString()).setScale(2, BigDecimal.ROUND_HALF_UP);
                    BigDecimal changeAmount = revAmountAfter.subtract(revAmountBefore).add(revPenaltyAmountAfter).subtract(revPenaltyAmountBefore);

                    if (changeAmount.compareTo(BigDecimal.ZERO) != 0) {
                        String rowkey = datas.get(0).get("rowkey").toString();
                        HBaseUtil.getInstance(hBaseConfig);
                        List<Cell> cells = HBaseUtil.getDataByKey(tableName, rowkey);
                        //hbase中已存的金额
                        for (Cell cell : cells) {
                            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("total")) {
                                totalAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)))) {
                                monthAmount = new BigDecimal(Bytes.toString(CellUtil.cloneValue(cell))).setScale(2, RoundingMode.HALF_UP);
                            } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("remark")) {
                                remark = Bytes.toString(CellUtil.cloneValue(cell));
                            }
                        }
                        //更新hbase中的数据
                        if (totalAmount != null) {
                            HBaseUtil.getInstance(hBaseConfig);
                            HBaseUtil.putData(tableName, rowkey, "data", "total", totalAmount.add(changeAmount).toString());
                        } else {
                            logger.error("数据异常，修改的数据在hbase中没有对应的total字段，或total值为null！");
                        }
                        if (monthAmount != null) {
                            HBaseUtil.getInstance(hBaseConfig);
                            HBaseUtil.putData(tableName, rowkey, "data", monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)), monthAmount.add(changeAmount).toString());
                        } else {
                            logger.error("数据异常，修改的数据在hbase中没有对应的{}字段，或{}值为null！", monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)), monthMap.get(value.f1.get("FPeriod").toString().substring(5, 7)));
                        }
                        if (StringUtils.isNotEmpty(remarkAfter)) {
                            HBaseUtil.getInstance(hBaseConfig);
                            HBaseUtil.putData(tableName, rowkey, "data", "remark", StringUtils.isEmpty(remark) ? remarkAfter : remark + "," + remarkAfter);//更新备注
                        }
                    }
                }
                return null;
            }
        });

        environment.execute("transferBillJob");
    }

    private static void newDataInsert(ClientInterface clientByXML, String tableName, Map<String,Object> rowData,String table) throws IOException {
        Map<String, Object> columns = new HashMap<>();
        String rowkey = null;
        //SQL获取需要插入的数据，赋值给columns
        TransferBillVo transferBillVo = new TransferBillVo();
        transferBillVo.setID(rowData.get("FID").toString());
        transferBillVo.setTable(table);//适配分表
        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        ITransferBillDao transferBillDao = sqlSession.getMapper(ITransferBillDao.class);
        //调用对应的方法获取数据
        columns = transferBillDao.getData(transferBillVo);
        Map<String, String> hbaseColumnMap = new HashMap<>();
        Map<String, String> esColumnsMap = new HashMap<>();
        for (Map.Entry<String, Object> column:columns.entrySet()){
            if (column.getValue().toString().equals("0E-10") || column.getValue() == null){
                continue;
            }
            if (column.getKey().equals("rowkey")){
                rowkey = column.getValue().toString();
                continue;
            }
            hbaseColumnMap.put(column.getKey(),column.getValue().toString());
        }
        if (StringUtils.isNotEmpty(rowkey)){
            esColumnsMap.put("rowkey",rowkey);
            esColumnsMap.put("moneyDefineID",hbaseColumnMap.get("moneyDefineID"));
            esColumnsMap.put("year",hbaseColumnMap.get("year"));
            esColumnsMap.put("roomID",hbaseColumnMap.get("roomID"));
            esColumnsMap.put("roomName",hbaseColumnMap.get("roomName"));
            esColumnsMap.put("customerName",hbaseColumnMap.get("customerName"));
            esColumnsMap.put("productTypeID",hbaseColumnMap.get("productTypeID"));
            esColumnsMap.put("buildingID",hbaseColumnMap.get("buildingID"));
            esColumnsMap.put("buildingName",hbaseColumnMap.get("buildingName"));
            esColumnsMap.put("customerID",hbaseColumnMap.get("customerID"));
            esColumnsMap.put("transferType",hbaseColumnMap.get("transferType"));
            esColumnsMap.put("projectName",hbaseColumnMap.get("projectName"));
            esColumnsMap.put("projectID",hbaseColumnMap.get("projectID"));
            //提交事务
            sqlSession.commit();
            //回滚事务，一般可以捕获异常，在发生Exception的时候，事务进行回滚
            sqlSession.rollback();
            //把数据插入到hbase
            HBaseUtil.getInstance(hBaseConfig);
            HBaseUtil.insertCols(tableName,rowkey,"data",hbaseColumnMap);
            logger.info("hbase插入完成，rowkey:"+rowkey);
            //把数据插入到es
            clientByXML.addDocument("hkey_transferbill","data",esColumnsMap);
            logger.info("es数据插入完成，rowkey:"+rowkey);
        }else {
            logger.info("没有可插入数据");
        }
    }
    static class MybatisSessionFactory {
        private static final Logger LOG = LoggerFactory.getLogger(MybatisSessionFactory.class);
        private static SqlSessionFactory sqlSessionFactory;
        private MybatisSessionFactory(){
            super();
        }
        public synchronized static SqlSessionFactory getSqlSessionFactory(){
            if(null==sqlSessionFactory){
                InputStream inputStream=null;
                try{
                    inputStream = MybatisSessionFactory.class.getClassLoader().getResourceAsStream("mybatis-config.xml");
                    sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
                }
                catch (Exception e){
                    LOG.error("create MybatisSessionFactory read mybatis-config.xml cause Exception",e);
                }
                if(null!=sqlSessionFactory){
                    LOG.info("get Mybatis sqlsession sucessed....");
                }
                else {
                    LOG.info("get Mybatis sqlsession failed....");
                }
            }
            return sqlSessionFactory;
        }
    }
}






















