package results;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

public class Result2 {


    public Float S_ACCTBAL;
    public String S_NAME;
    public String N_NAME;
    public Integer P_PARTKEY;
    public String P_MFGR;
    public String S_ADDRESS;
    public String S_PHONE;
    public String S_COMMENT;

    public Result2() {
    }

    public Float getS_ACCTBAL() {
        return S_ACCTBAL;
    }

    public void setS_ACCTBAL(Float s_ACCTBAL) {
        S_ACCTBAL = s_ACCTBAL;
    }

    public String getS_NAME() {
        return S_NAME;
    }

    public void setS_NAME(String s_NAME) {
        S_NAME = s_NAME;
    }

    public String getN_NAME() {
        return N_NAME;
    }

    public void setN_NAME(String n_NAME) {
        N_NAME = n_NAME;
    }

    public Integer getP_PARTKEY() {
        return P_PARTKEY;
    }

    public void setP_PARTKEY(Integer p_PARTKEY) {
        P_PARTKEY = p_PARTKEY;
    }

    public String getP_MFGR() {
        return P_MFGR;
    }

    public void setP_MFGR(String p_MFGR) {
        P_MFGR = p_MFGR;
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

    public String getS_COMMENT() {
        return S_COMMENT;
    }

    public void setS_COMMENT(String s_COMMENT) {
        S_COMMENT = s_COMMENT;
    }

    public static Tuple8<Float,String,String,Integer,String,String,String,String> toTuple(Result2 result2) {
        return Tuple8.of(result2.getS_ACCTBAL(),result2.getS_NAME(),result2.getN_NAME(),result2.getP_PARTKEY(),result2.getP_MFGR(),
                result2.getS_ADDRESS(),result2.getS_PHONE(),result2.getS_COMMENT());
    }
}
