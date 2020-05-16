import org.junit.Test;
import utils.MD5Utils;
import utils.RowKeyUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: wangsen
 * @Date: 2020/5/13 12:24
 * @Description:
 **/
public class TestGenerateRowkey {
    @Test
    public void test(){
        System.out.println(MD5Utils.hash("sadaed122e1234dsadasda342dafdaf123rfasfa"));
        System.out.println(RowKeyUtil.generateShortUuid8(MD5Utils.hash("sadaed122e1234dsadasda342dafdaf123rfasfa")));
        System.out.println(RowKeyUtil.generateShortUuid8(MD5Utils.hash("sadaed122e1234dsadasda342dafdaf123rfasfa")));
    }
}
