package table.dynamic;

import java.math.BigDecimal;

/**
 * @Author: wangsen
 * @Date: 2020/4/10 15:20
 * @Description:
 **/
public class UserVo {
    private String user;
    private BigDecimal countage;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public BigDecimal getCountage() {
        return countage;
    }

    public void setCountage(BigDecimal countage) {
        this.countage = countage;
    }
    @Override
    public String toString(){
        return "user:"+user+",countage:"+countage;
    }
}
