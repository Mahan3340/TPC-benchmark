package results;

import org.apache.flink.api.java.tuple.Tuple1;

public class Result19 {

   public Float REVENUE;


    public Result19() {
    }

    public Float getREVENUE() {
        return REVENUE;
    }

    public void setREVENUE(Float REVENUE) {
        this.REVENUE = REVENUE;
    }

    public static Tuple1<Float> toTuple(Result19 result19) {
        return Tuple1.of(result19.getREVENUE());
    }
}
