package results;

import org.apache.flink.api.java.tuple.Tuple1;

public class Result6 {

    public Float REVENUE;
    public Result6() {}

    public Float getREVENUE() {
        return REVENUE;
    }

    public void setREVENUE(Float REVENUE) {
        this.REVENUE = REVENUE;
    }

    public static Tuple1<Float> toTuple(Result6 result6) {
        return Tuple1.of(result6.getREVENUE());
    }
}
