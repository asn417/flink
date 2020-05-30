package Flink;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @Author: wangsen
 * @Date: 2020/5/30 17:27
 * @Description:
 **/
@Mapper
@Component
public interface ITransferBillDao {
    public Map<String,Object> getData(TransferBillVo transferBillVo);
}
