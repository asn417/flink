package Flink;

import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wangsen
 * @Date: 2020/5/30 17:22
 * @Description:
 **/
public class ESUtils {
    /**
     * @Author: wangsen
     * @Description: 组合index、type和method
     * @Date: 2020/1/17
     * @Param:
     * @Return:
     **/
    public static String getPath(String index,String type,String method){
        StringBuilder builder = new StringBuilder();
        String path = "";
        path = builder.append(index).append("/").append(type).append("/").append(method).toString();
        return path;
    }

    /**
     * @Author: wangsen
     * @Description: 修改最大返回数据量max_result_window
     * @Date: 2020/1/13
     * @Param:
     * @Return:
     **/
    public static void setTypeMaxResultWindow(ClientInterface client, String indice, int value){
        String key = "max_result_window";
        client.updateIndiceSetting(indice,key,value);
    }
    public static void setIndexMaxResultWindow(ClientInterface client, String indice,int value){
        String key = "index.max_result_window";
        client.updateIndiceSetting(indice,key,value);
    }

    /***
     * @Author: wangsen
     * @Description: scroll一次性获取全部数据
     * @Date: 2020/3/20
     * @Param: [client, requestHead, paramVo, mapperMethod, resultType]
     * @Return: org.frameworkset.elasticsearch.entity.ESDatas<T>
     **/
    public static <T,E> ESDatas<T> getAllDataByParam(ClientInterface client, String requestHead, E paramVo, String mapperMethod, Class<T> resultType){
        ESDatas<T> esDatas = null;//返回的文档封装对象类型
        esDatas = client.scroll(requestHead,mapperMethod,"1m",paramVo,resultType);
        return esDatas;
    }

    /**
     * @Author: wangsen
     * @Description: 获取索引中文档数量
     * @Date: 2020/1/17
     * @Param:
     * @Return:
     **/
    public static long getDocumentsNumOfIndex(ClientInterface client,String index){
        return client.countAll(index);
    }

    /**
     * @Author: wangsen
     * @Description: 获取索引中存入的全部数据，默认分5000条记录一批从es获取数据
     * @Date: 2020/1/17
     * @Param:
     * @Return:
     **/
    public static <T> List<T> getDatasOfIndex(ClientInterface client, String index, Class<T> type){
        List<T> dataList = new ArrayList<>();
        ESDatas<T> esDatas = client.searchAll(index,type);
        dataList = esDatas.getDatas();
        return dataList;
    }

    /**
     * @Author: wangsen
     * @Description: 获取指定条数的数据
     * @Date: 2020/1/17
     * @Param:
     * @Return:
     **/
    public static <T> List<T> getDatasOfIndex(ClientInterface client,String index,int fetchSize,Class<T> type){
        List<T> dataList = new ArrayList<>();
        ESDatas<T> esDatas = client.searchAll(index,fetchSize,type);
        dataList = esDatas.getDatas();
        return dataList;
    }

    /**
     * @Author: wangsen
     * @Description: 指定线程数并行获取数据(查询速度会快很多)
     * @Date: 2020/1/17
     * @Param:
     * @Return:
     **/
    public static <T> List<T> getDatasByParallel(ClientInterface client,String index,int parallelNum,Class<T> type){
        ESDatas<T> esDatas = client.searchAllParallel(index,type,parallelNum);
        return esDatas.getDatas();
    }
    public static <T> List<T> getDatasByParallel(ClientInterface client,String index,int fetchSize,int parallelNum,Class<T> type){
        ESDatas<T> esDatas = client.searchAllParallel(index,fetchSize,type,parallelNum);
        return esDatas.getDatas();
    }

    /***
     * @Author: wangsen
     * @Description: 批量插入
     * @Date: 2020/3/13
     * @Param: [client, index, dataList]
     * @Return: void
     **/
    public static <T> String batchPut(ClientInterface client,String index,String type,List<T> dataList){
        return client.addDocuments(index,type,dataList);
    }


}
