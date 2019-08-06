package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import tables.Nation;

public class Result5 {

    public Float REVENUE;
    public Result5() {}

    public Float getREVENUE() {
        return REVENUE;
    }

    public void setREVENUE(Float REVENUE) {
        this.REVENUE = REVENUE;
    }

    public static Tuple1<Float> toTuple(Result5 result5) {
        return Tuple1.of(result5.getREVENUE());
    }
}
