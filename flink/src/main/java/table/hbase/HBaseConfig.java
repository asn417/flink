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
    private final String hadoop_home_dir;
    private final String hbase_zookeeper_quorum;
    private final String hbase_master;
    private final String clientport;


    public HBaseConfig(Config config){
        this.config = config;

        hadoop_home_dir = config.getProperty("hadoop.home.dir","D:/MySoftware/hadoop-2.8.5");

        hbase_zookeeper_quorum = config.getProperty("hbase.zookeeper.quorum","172.20.184.17");

        hbase_master = config.getProperty("hbase.master","172.20.184.17:16000");

        clientport = config.getProperty("hbase.zookeeper.property.clientPort","2181");
    }

    public String getHadoop_home_dir() {
        return this.hadoop_home_dir;
    }

    public String getHbase_zookeeper_quorum() {
        return this.hbase_zookeeper_quorum;
    }

    public String getHbase_master() {
        return this.hbase_master;
    }

    public String getClientport() {
        return this.clientport;
    }
}
