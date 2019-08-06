package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;

public class Result17 {


   public Float AVG_YEARLY;



    public Result17() {
    }


    public Float getAVG_YEARLY() {
        return AVG_YEARLY;
    }

    public void setAVG_YEARLY(Float AVG_YEARLY) {
        this.AVG_YEARLY = AVG_YEARLY;
    }

    public static Tuple1<Float> toTuple(Result17 result17) {
        return Tuple1.of(result17.getAVG_YEARLY());
    }
}
