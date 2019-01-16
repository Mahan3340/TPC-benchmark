package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple8;

public class Result10 {

    public Integer C_CUSTKEY;
    public String C_NAME;
    public Float REVENUE;
    public Float C_ACCTBAL;
    public String N_NAME;
    public String C_ADDRESS;
    public String C_PHONE;
    public String C_COMMENT;

    public Result10() {
    }

    public Integer getC_CUSTKEY() {
        return C_CUSTKEY;
    }

    public void setC_CUSTKEY(Integer c_CUSTKEY) {
        C_CUSTKEY = c_CUSTKEY;
    }

    public String getC_NAME() {
        return C_NAME;
    }

    public void setC_NAME(String c_NAME) {
        C_NAME = c_NAME;
    }

    public Float getREVENUE() {
        return REVENUE;
    }

    public void setREVENUE(Float REVENUE) {
        this.REVENUE = REVENUE;
    }

    public Float getC_ACCTBAL() {
        return C_ACCTBAL;
    }

    public void setC_ACCTBAL(Float c_ACCTBAL) {
        C_ACCTBAL = c_ACCTBAL;
    }

    public String getN_NAME() {
        return N_NAME;
    }

    public void setN_NAME(String n_NAME) {
        N_NAME = n_NAME;
    }

    public String getC_ADDRESS() {
        return C_ADDRESS;
    }

    public void setC_ADDRESS(String c_ADDRESS) {
        C_ADDRESS = c_ADDRESS;
    }

    public String getC_PHONE() {
        return C_PHONE;
    }

    public void setC_PHONE(String c_PHONE) {
        C_PHONE = c_PHONE;
    }

    public String getC_COMMENT() {
        return C_COMMENT;
    }

    public void setC_COMMENT(String c_COMMENT) {
        C_COMMENT = c_COMMENT;
    }

    public static Tuple8<Integer,String,Float,Float,String,String,String,String> toTuple(Result10 result10) {
        return Tuple8.of(result10.getC_CUSTKEY(),result10.getC_NAME(),result10.getREVENUE(),result10.getC_ACCTBAL(),result10.getC_NAME()
        ,result10.getC_ADDRESS(),result10.getC_PHONE(),result10.getC_COMMENT());
    }
}
