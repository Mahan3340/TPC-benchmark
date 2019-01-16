package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;

public class Result15 {

    public Integer S_SUPPKEY;
    public String S_NAME;
    public String S_ADDRESS;
    public String S_PHONE;
    public Float TOTAL;



    public Result15() {
    }

    public Integer getS_SUPPKEY() {
        return S_SUPPKEY;
    }

    public void setS_SUPPKEY(Integer s_SUPPKEY) {
        S_SUPPKEY = s_SUPPKEY;
    }

    public String getS_NAME() {
        return S_NAME;
    }

    public void setS_NAME(String s_NAME) {
        S_NAME = s_NAME;
    }

    public String getS_ADDRESS() {
        return S_ADDRESS;
    }

    public void setS_ADDRESS(String s_ADDRESS) {
        S_ADDRESS = s_ADDRESS;
    }

    public String getS_PHONE() {
        return S_PHONE;
    }

    public void setS_PHONE(String s_PHONE) {
        S_PHONE = s_PHONE;
    }

    public Float getTOTAL() {
        return TOTAL;
    }

    public void setTOTAL(Float TOTAL) {
        this.TOTAL = TOTAL;
    }

    public static Tuple5<Integer,String,String,String,Float> toTuple(Result15 result15) {
        return Tuple5.of(result15.getS_SUPPKEY(),result15.getS_NAME(),result15.getS_ADDRESS(),result15.getS_PHONE(),result15.TOTAL);
    }
}
