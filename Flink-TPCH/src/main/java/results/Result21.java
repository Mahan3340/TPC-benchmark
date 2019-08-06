package results;

import org.apache.flink.api.java.tuple.Tuple2;

public class Result21 {


    public String S_NAME;
    public Long NUM_WAIT;

    public Result21() {
    }

    public String getS_NAME() {
        return S_NAME;
    }

    public void setS_NAME(String s_NAME) {
        S_NAME = s_NAME;
    }

    public Long getNUM_WAIT() {
        return NUM_WAIT;
    }

    public void setNUM_WAIT(Long NUM_WAIT) {
        this.NUM_WAIT = NUM_WAIT;
    }

    public static Tuple2<String,Long> toTuple(Result21 result21) {
        return Tuple2.of(result21.getS_NAME(),result21.getNUM_WAIT());
    }
}
