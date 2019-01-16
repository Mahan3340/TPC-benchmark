package results;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Result12 {

    public String L_SHIPMODE;
    public Integer LOW_LINE_COUNT;
    public Integer HIGH_LINE_COUNT;


    public Result12() {
    }

    public String getL_SHIPMODE() {
        return L_SHIPMODE;
    }

    public void setL_SHIPMODE(String l_SHIPMODE) {
        L_SHIPMODE = l_SHIPMODE;
    }

    public Integer getLOW_LINE_COUNT() {
        return LOW_LINE_COUNT;
    }

    public void setLOW_LINE_COUNT(Integer LOW_LINE_COUNT) {
        this.LOW_LINE_COUNT = LOW_LINE_COUNT;
    }

    public Integer getHIGH_LINE_COUNT() {
        return HIGH_LINE_COUNT;
    }

    public void setHIGH_LINE_COUNT(Integer HIGH_LINE_COUNT) {
        this.HIGH_LINE_COUNT = HIGH_LINE_COUNT;
    }

    public static Tuple3<String,Integer,Integer> toTuple(Result12 result12) {
        return Tuple3.of(result12.getL_SHIPMODE(),result12.getLOW_LINE_COUNT(),result12.getHIGH_LINE_COUNT());
    }
}
