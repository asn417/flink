package utils;

import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import table.hbase.HBaseConfig;

import java.io.IOException;
import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/30 17:10
 * @Description:
 **/
public class HBaseUtil {
    private static Connection connection;
    private static Configuration configuration;
    private static HBaseUtil hBaseUtils;
    private static Properties properties;
    private static Admin admin;

    /**
     * @Author: wangsen
     * @Description: 唯一实例，线程安全，保证连接池唯一
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static HBaseUtil getInstance(HBaseConfig hBaseConfig) {
        if (hBaseUtils == null) {
            synchronized (HBaseUtil.class) {
                if (hBaseUtils == null) {
                    hBaseUtils = new HBaseUtil();
                    hBaseUtils.init(hBaseConfig);
                }
            }
        }
        return hBaseUtils;
    }

    /**
     * @Author: wangsen
     * @Description: 创建连接池并初始化环境配置
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public void init(HBaseConfig hBaseConfig) {
        properties = System.getProperties();
        // 实例化HBase配置类
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
        }
        try {
            // 加载本地hadoop二进制包，换成你解压的地址
            properties.setProperty("hadoop.home.dir", hBaseConfig.getHADOOP_HOME_DIR());
            // zookeeper集群的URL配置信息
            //configuration.set("hbase.zookeeper.quorum", "flink1,flink2,flink3");
            configuration.set("hbase.zookeeper.quorum", hBaseConfig.getHBASE_ZOOKEEPER_QUORUM());

            // HBase的Master
            //configuration.set("hbase.master", "flink1:16000");
            configuration.set("hbase.master", hBaseConfig.getHBASE_MASTER());
            // 客户端连接zookeeper端口
            //configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.property.clientPort", hBaseConfig.getCLIENTPORT());
            // 获取hbase连接对象*/
            if (connection == null || connection.isClosed()) {
                connection = ConnectionFactory.createConnection(configuration);
                admin = connection.getAdmin();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Author: wangsen
     * @Description: 创建表
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static void createTable(String tableName, String[] columnFamily) throws IOException{
        TableName name = TableName.valueOf(tableName);
        //如果存在则删除
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
            //logger.error("create htable error! this table {} already exists!", name);
        } else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(name);
            for (String cf : columnFamily) {
                tableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build());
            }
            admin.createTable(tableDescriptor.build());//创建表
        }
    }

    /**
     * @Author: wangsen
     * @Description: 关闭连接池
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static void close() {
        try {
            if (connection != null)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Author: wangsen
     * @Description: 私有无参构造方法
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    private HBaseUtil() {
    }

    /**
     * @Author: wangsen
     * @Description: 插入单条数据
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static void insertCols(String tablename, String rowkey, String family, Map<String, String> columns)
            throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Put put = new Put(rowkey.getBytes());
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                put.addColumn(family.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
    }

    /**
     * @Author: wangsen
     * @Description: 插入单条数据
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static void insertCols(String tablename, String rowkey, String columnFamily, String[] columns,
                                  String[] values) throws IOException {
        if (values==null || values.length==0){
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }
        table.put(put);
        table.close();
    }


    /**
     * @Author: wangsen
     * @Description: 插入单条数据
     * @Date: 2020/1/16
     * @Param: values的key为rowkey，值为column和对应的value
     * @Return:
     **/
    public static void insertCols(String tableName,String columnFamily,Map<String,Map<String,String>> values) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            for (Map.Entry<String,Map<String,String>> entry:values.entrySet()){
                Put put = new Put(Bytes.toBytes(entry.getKey()));
                for (Map.Entry<String,String> subEntry:entry.getValue().entrySet()){
                    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(subEntry.getKey()),Bytes.toBytes(subEntry.getValue()));
                }
                puts.add(put);
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
        }
    }
    public static void batchInsert(String tableName,String columnFamily,Map<String,Map<String,String>> values,int batchSize) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            //Iterables.partition(values.entrySet(), batchSize)这个方法会把数据复制到新的对象，造成内存浪费，后面可以优化
            for(List<Map.Entry<String, Map<String, String>>> entries : Iterables.partition(values.entrySet(), batchSize)){
                List<Put> puts = new ArrayList<>();
                for (Map.Entry<String,Map<String,String>> entry : entries){
                    Put put = new Put(Bytes.toBytes(entry.getKey()));
                    for (Map.Entry<String,String> subEntry:entry.getValue().entrySet()){
                        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(subEntry.getKey()),Bytes.toBytes(subEntry.getValue()));
                    }
                    puts.add(put);
                }
                table.put(puts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
        }
    }

    /**
     * @Author: wangsen
     * @Description: 获取单条数据
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static Result getRow(String tableName, String row) throws IOException {
        Table table = null;
        Result result = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(row.getBytes());
            result = table.get(get);
        } finally {
            table.close();
        }
        return result;
    }

    /**
     * @Author: wangsen
     * @Description: 查询多行信息
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static Result[] getRows(String tablename, List<byte[]> rows) throws IOException {
        Table table = null;
        List<Get> gets = null;
        Result[] results = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            gets = new ArrayList<Get>();
            for (byte[] row : rows) {
                if (row != null) {
                    gets.add(new Get(row));
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
        return results;
    }


    /**
     * @Author: wangsen
     * @Description: 获取整表数据
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static ResultScanner scanTable(String tablename) throws IOException {
        Table table = null;
        ResultScanner results = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();
            scan.setCaching(1000);
            results = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
        return results;
    }

    /**
     * @Author: wangsen
     * @Description: 获取表对象
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static Connection getConnection() {
        return connection;
    }

    /**
     * @Author: wangsen
     * @Description: 删除数据
     * @Date: 2020/1/12
     * @Param:
     * @Return:
     **/
    public static void deleteCol(String tablename, String family, String column, String row) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Delete del = new Delete(row.getBytes());
            del.addColumns(family.getBytes(), column.getBytes());
            table.delete(del);
        } finally {
            table.close();
        }
    }


    /**
     * @Author: wangsen
     * @Description: 根据rowkey删除整行数据
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static void deleteRow(String tablename, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }

    /**
     * @Author: wangsen
     * @Description: 判断表是否存在
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * @Author: wangsen
     * @Description: 判断列族是否存在
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static boolean isColumnFamilyExist(String tableName,String cf) throws IOException {
        if(isTableExist(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            TableDescriptor tableDescriptor = table.getDescriptor();
            ColumnFamilyDescriptor descriptor = tableDescriptor.getColumnFamily(Bytes.toBytes(cf));
            return descriptor==null?false:true;
        }else {
            return false;
        }
    }

    /**
     * @Author: wangsen
     * @Description: 根据表明获取regionInfo
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/

    public static RegionInfo getRegionInfo(String tableName){
        RegionInfoBuilder regionInfoBuilder = RegionInfoBuilder.newBuilder(TableName.valueOf(tableName));
        RegionInfo regionInfo = regionInfoBuilder.build();
        String regionname = Bytes.toString(regionInfo.getRegionName());
        String strkey = Bytes.toString(regionInfo.getStartKey());
        String endkey = Bytes.toString(regionInfo.getEndKey());
        System.out.println("RegionName:"+regionname+" ,START:"+strkey+" ,END:"+endkey);
        return regionInfo;
    }

    /**
     * @Author: wangsen
     * @Description: 删除表
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static void droptable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * @Author: wangsen
     * @Description: 构造rowkey，共24位
     * 向hbase插入数据，需要设计rowkey（hbase的数据是根据rowkey的字典排序进行存储的）
     * rowkey设计原则
     * 1.长度尽量短
     * 2.唯一性
     * @Date: 2020/1/17
     * @Param:
     * @Return:
     **/
    public static String generateRowkey1(String interfaceNum,String ecID,String index){
        StringBuilder id = new StringBuilder();
        Random random = new Random();
        for (int i=0;i<2;i++){
            id.append(random.nextInt(10));
        }
        id.append(interfaceNum).append(ecID.substring(0,8)).append(index.substring(index.length()-10).replace(".","")).append(RandomStringUtils.randomAlphanumeric(2));
        return id.toString();
    }


    public static String generateRowKey(String str){
        int sum = 0;
        byte[] bytes = str.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            sum += bytes[i];
        }
        return (sum % 9) + str + REIDIdentifier.generateShortUuid5();
    }

    public static byte[][] getSplitKeys(String[] keys) {
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
    /**
     * 创建预分区hbase表
     * @param tableName 表名
     * @param columnFamily 列簇
     * @return
     */
    public static void createTableBySplitKeys(String tableName, List<String> columnFamily,byte[][] splitKeys) throws IOException {
        if (StringUtils.isBlank(tableName) || columnFamily == null
                || columnFamily.size() < 0) {
            System.out.println("===Parameters tableName|columnFamily should not be null,Please check!===");
            return;
        }
        if (isTableExist(tableName)) {
            System.out.println(tableName+"表已存在！");
            return;
        }
        TableDescriptorBuilder tdesc=TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for(String s: columnFamily){
            ColumnFamilyDescriptor cfd=ColumnFamilyDescriptorBuilder.of(s);
            tdesc.setColumnFamily(cfd);
        }
        TableDescriptor desc=tdesc.build();
        admin.createTable(desc,splitKeys);
        System.out.println("===Create Table " + tableName
                + " Success!columnFamily:" + columnFamily.toString()
                + "===");

    }
    /**
     * 创建命名空间
     * @param namespace
     */
    public static void createNamespace(String namespace) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(namespace+"命名空间已存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除命名空间
     * @param namespace
     * @param force
     */
    public static void dropNamespace(String namespace, boolean force) {
        try {
            if (force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    /**
     * 删除表
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)){
            System.out.println(tableName+"表不存在");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 插入单条数据
     * @param tableName
     * @param rowkey
     * @param cf
     * @param cn
     * @param value
     * @throws IOException
     */
    public static void putData(String tableName,String rowkey,String cf,String cn,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 获取单条数据
     * @param tableName
     * @param rowkey
     */
    public static List<Cell> getDataByKey(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        for (Cell cell:result.rawCells()){
            System.out.println("cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        List<Cell> listCells = result.listCells();
        table.close();
        return listCells;
    }


    /**
     * 根据rowkey范围扫描过滤
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public static void scanTable(String tableName,String startRow,String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        int rowCount = 0;
        for (Result result:resultScanner){
            for (Cell cell : result.rawCells()) {
                System.out.println("--------------------------------rowkey:"+Bytes.toString(CellUtil.cloneRow(cell))+",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
            rowCount++;
        }
        System.out.println("--------------------------------rowcount: "+rowCount+" -------------------------------------");
        table.close();
    }
    /**
     * 根据rowKey过滤数据，rowKey可以使用正则表达式
     * 返回rowKey和Cells的键值对
     * @param tableName
     * @param rowkey
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByRowKeyRegex(String tableName, String rowkey, CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //使用正则
        RowFilter filter = new RowFilter(operator,new RegexStringComparator(rowkey));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        table.close();
        return map;
    }

    /**
     * 包含子串匹配(判断一个子串是否存在于rowkey中，并且不区分大小写)
     * @param tableName
     * @param rowkey
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByRowKeySub(String tableName, String rowkey, CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(operator,new SubstringComparator(rowkey));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        table.close();
        return map;
    }

    /**
     * 使用二进制比较器BinaryComparator，提高效率(只能是完整的rowkey)
     * @param tableName
     * @param rowkey
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByRowKeyBinary(String tableName, String rowkey, CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(operator,new BinaryComparator(Bytes.toBytes(rowkey)));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        table.close();
        return map;
    }
    /**
     * 根据列族，列名，列值（支持正则）查找数据
     * 返回值：如果查询到值，会返回所有匹配的rowKey下的各列族、列名的所有数据（即使查询的时候这些列族和列名并不匹配）
     * @param tableName
     * @param columnFamily
     * @param columnName
     * @param value
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByValueRegex(String tableName,String columnFamily,String columnName,
                                                            String value,CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        //正则匹配
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),operator,new RegexStringComparator(value));

        //完全匹配
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(columnName),operator,Bytes.toBytes(value));

        //SingleColumnValueExcludeFilter排除列值

        //要过滤的列必须存在，如果不存在，那么这些列不存在的数据也会返回。如果不想让这些数据返回,设置setFilterIfMissing为true
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        return map;
    }

    /**
     * 根据传入的value值精准匹配
     * @param tableName
     * @param columnFamily
     * @param columnName
     * @param value
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByValueBytes(String tableName,String columnFamily,String columnName,
                                                            String value,CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //完全匹配
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName),operator,Bytes.toBytes(value));
        //要过滤的列必须存在，如果不存在，那么这些列不存在的数据也会返回。如果不想让这些数据返回,设置setFilterIfMissing为true
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        return map;
    }
    /**
     * 根据列名前缀过滤数据
     * @param tableName
     * @param prefix
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByColumnPrefix(String tableName,String prefix) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));

        //QualifierFilter 用于列名多样性匹配过滤
//        QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL,new SubstringComparator(prefix));

        //多个列名前缀匹配
//        MultipleColumnPrefixFilter multiFilter = new MultipleColumnPrefixFilter(new byte[][]{});

        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        return map;
    }

    /**
     * 过滤器集合的使用。
     * 根据列名范围以及列名前缀过滤数据
     * @param tableName
     * @param colPrefix
     * @param minCol
     * @param maxCol
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByPrefixAndRange(String tableName,String colPrefix,
                                                                String minCol,String maxCol) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(colPrefix));

        //列名范围扫描，上下限范围包括
        ColumnRangeFilter rangeFilter = new ColumnRangeFilter(Bytes.toBytes(minCol),true,
                Bytes.toBytes(maxCol),true);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter);
        filterList.addFilter(rangeFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
            System.out.println("-----------------rowkey: "+Bytes.toString(result.getRow()));
        }
        return map;
    }


    /**
     * 删除数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnName
     * @throws IOException
     */
    public static void deleteByKeyAndFC(String tableName,String rowkey,String columnFamily,String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey删除所有行数据
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public static void deleteByKey(String tableName,String rowkey) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey和列族删除所有行数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @throws IOException
     */
    public static void deleteByKeyAndFamily(String tableName,String rowkey,String columnFamily) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey、列族删除多个列的数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnNames
     * @throws IOException
     */
    public static void deleteByKeyAndFCList(String tableName,String rowkey, String columnFamily,List<String> columnNames) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        for(String columnName:columnNames){
            delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        }
        table.delete(delete);
        table.close();
    }
}
