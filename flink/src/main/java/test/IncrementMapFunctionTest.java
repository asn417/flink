package test;

import org.apache.flink.api.common.functions.MapFunction;
import org.junit.Test;

/**
 * @Author: wangsen
 * @Date: 2020/5/18 16:38
 * @Description: Unit Testing Stateless, Timeless UDFs
 **/
public class IncrementMapFunctionTest {
    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();

        // call the methods that you have implemented
        boolean bool = assertEquals(3L, incrementer.map(2L));
        System.out.println("========"+bool);

    }
    public class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(Long record) throws Exception {
            return record + 1;
        }
    }
    private boolean assertEquals(long l1,long l2){
        return l1 == l2;
    }
}
