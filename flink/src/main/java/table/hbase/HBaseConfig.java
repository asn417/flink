package table.hbase;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;

/**
 * @Author: wangsen
 * @Date: 2020/4/13 13:46
 * @Description:
 **/
public class HBaseConfig {
    private Config config;
    public HBaseConfig(Config config){
        this.config = config;
    }
    String HADOOP_HOME_DIR = config.getProperty("hadoop.home.dir","D:/MySoftware/hadoop-2.8.5");

    String HBASE_ZOOKEEPER_QUORUM = config.getProperty("hbase.zookeeper.quorum","172.20.184.17");

    String HBASE_MASTER = config.getProperty("hbase.master","172.20.184.17:16000");

    String CLIENTPORT = config.getProperty("hbase.zookeeper.property.clientPort","2181");

    public String getHADOOP_HOME_DIR() {
        return HADOOP_HOME_DIR;
    }

    public void setHADOOP_HOME_DIR(String HADOOP_HOME_DIR) {
        this.HADOOP_HOME_DIR = HADOOP_HOME_DIR;
    }

    public String getHBASE_ZOOKEEPER_QUORUM() {
        return HBASE_ZOOKEEPER_QUORUM;
    }

    public void setHBASE_ZOOKEEPER_QUORUM(String HBASE_ZOOKEEPER_QUORUM) {
        this.HBASE_ZOOKEEPER_QUORUM = HBASE_ZOOKEEPER_QUORUM;
    }

    public String getHBASE_MASTER() {
        return HBASE_MASTER;
    }

    public void setHBASE_MASTER(String HBASE_MASTER) {
        this.HBASE_MASTER = HBASE_MASTER;
    }

    public String getCLIENTPORT() {
        return CLIENTPORT;
    }

    public void setCLIENTPORT(String CLIENTPORT) {
        this.CLIENTPORT = CLIENTPORT;
    }

}
