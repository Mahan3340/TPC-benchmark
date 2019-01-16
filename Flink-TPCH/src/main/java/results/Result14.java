package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class Result14 {

    public Float PROMO_REVENUE;



    public Result14() {
    }

    public Float getPROMO_REVENUE() {
        return PROMO_REVENUE;
    }

    public void setPROMO_REVENUE(Float PROMO_REVENUE) {
        this.PROMO_REVENUE = PROMO_REVENUE;
    }

    public static Tuple1<Float> toTuple(Result14 result14) {
        return Tuple1.of(result14.getPROMO_REVENUE());
    }
}
