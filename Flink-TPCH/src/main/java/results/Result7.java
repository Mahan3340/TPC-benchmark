package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;

public class Result7 {

    public Float REVENUE;
    public String SUPP_NATION;
    public String CUST_NATION;
    public String L_SHIPDATE;

    public Result7() {}

    public Float getREVENUE() {
        return REVENUE;
    }

    public void setREVENUE(Float REVENUE) {
        this.REVENUE = REVENUE;
    }

    public String getSUPP_NATION() {
        return SUPP_NATION;
    }

    public void setSUPP_NATION(String SUPP_NATION) {
        this.SUPP_NATION = SUPP_NATION;
    }

    public String getCUST_NATION() {
        return CUST_NATION;
    }

    public void setCUST_NATION(String CUST_NATION) {
        this.CUST_NATION = CUST_NATION;
    }

    public String getL_SHIPDATE() {
        return L_SHIPDATE;
    }

    public void setL_SHIPDATE(String l_SHIPDATE) {
        L_SHIPDATE = l_SHIPDATE;
    }

    public static Tuple4<Float,String,String,String> toTuple(Result7 result7) {
        return Tuple4.of(result7.getREVENUE(),result7.getSUPP_NATION(),result7.getCUST_NATION(),result7.getL_SHIPDATE());
    }
}
