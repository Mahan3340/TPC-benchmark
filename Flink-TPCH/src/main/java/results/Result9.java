package results;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class Result9 {

    public Float SUM_PROFIT;
    public String O_YEAR;
    public String N_NAME;


    public Result9() {}

    public Float getSUM_PROFIT() {
        return SUM_PROFIT;
    }

    public void setSUM_PROFIT(Float SUM_PROFIT) {
        this.SUM_PROFIT = SUM_PROFIT;
    }

    public String getO_YEAR() {
        return O_YEAR;
    }

    public void setO_YEAR(String o_YEAR) {
        O_YEAR = o_YEAR;
    }

    public String getN_NAME() {
        return N_NAME;
    }

    public void setN_NAME(String n_NAME) {
        N_NAME = n_NAME;
    }

    public static Tuple3<Float,String,String> toTuple(Result9 result9) {
        return Tuple3.of(result9.getSUM_PROFIT(),result9.getO_YEAR(),result9.getN_NAME());
    }
}
