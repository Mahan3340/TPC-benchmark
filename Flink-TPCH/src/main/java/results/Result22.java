package results;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Result22 {


    public Long ACCTBAL_COUNT;
    public Float ACCTBAL_SUM;
    public String COUNTRY_CODE;


    public Result22() {
    }

    public java.lang.Long getACCTBAL_COUNT() {
        return ACCTBAL_COUNT;
    }

    public void setACCTBAL_COUNT(java.lang.Long ACCTBAL_COUNT) {
        this.ACCTBAL_COUNT = ACCTBAL_COUNT;
    }

    public Float getACCTBAL_SUM() {
        return ACCTBAL_SUM;
    }

    public void setACCTBAL_SUM(Float ACCTBAL_SUM) {
        this.ACCTBAL_SUM = ACCTBAL_SUM;
    }

    public String getCOUNTRY_CODE() {
        return COUNTRY_CODE;
    }

    public void setCOUNTRY_CODE(String COUNTRY_CODE) {
        this.COUNTRY_CODE = COUNTRY_CODE;
    }

    public static Tuple3<Long,Float,String> toTuple(Result22 result22) {
        return Tuple3.of(result22.getACCTBAL_COUNT(),result22.getACCTBAL_SUM(),result22.getCOUNTRY_CODE());
    }
}
