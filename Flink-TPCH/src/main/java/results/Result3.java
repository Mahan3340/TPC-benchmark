package results;

import org.apache.flink.api.java.tuple.Tuple2;

public class Result3 {

    public String O_ORDERDATE;
    public Float REVENUE;

    public Result3() {}

    public String getO_ORDERDATE() {
        return O_ORDERDATE;
    }

    public void setO_ORDERDATE(String o_ORDERDATE) {
        O_ORDERDATE = o_ORDERDATE;
    }

    public Float getREVENUE() {
        return REVENUE;
    }

    public void setREVENUE(Float REVENUE) {
        this.REVENUE = REVENUE;
    }

    public static Tuple2<String,Float> toTuple(Result3 result3) {
        return Tuple2.of(result3.getO_ORDERDATE(),result3.getREVENUE());
    }
}
