package results;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class Result16 {


    public Long SUPPLIER_COUNT;
    public String P_BRAND;
    public String P_TYPE;
    public Integer P_SIZE;



    public Result16() {
    }

    public Long getSUPPLIER_COUNT() {
        return SUPPLIER_COUNT;
    }

    public void setSUPPLIER_COUNT(Long SUPPLIER_COUNT) {
        this.SUPPLIER_COUNT = SUPPLIER_COUNT;
    }

    public String getP_BRAND() {
        return P_BRAND;
    }

    public void setP_BRAND(String p_BRAND) {
        P_BRAND = p_BRAND;
    }

    public String getP_TYPE() {
        return P_TYPE;
    }

    public void setP_TYPE(String p_TYPE) {
        P_TYPE = p_TYPE;
    }

    public Integer getP_SIZE() {
        return P_SIZE;
    }

    public void setP_SIZE(Integer p_SIZE) {
        P_SIZE = p_SIZE;
    }


    public static Tuple4<Long,String,String,Integer> toTuple(Result16 result16) {
        return Tuple4.of(result16.getSUPPLIER_COUNT(),result16.P_BRAND,result16.getP_TYPE(),result16.P_SIZE);
    }
}
