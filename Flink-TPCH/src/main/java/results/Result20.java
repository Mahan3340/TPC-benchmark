package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class Result20 {


    public String S_NAME;
    public String S_ADDRESS;

    public Result20() {
    }

    public String getS_NAME() {
        return S_NAME;
    }

    public void setS_NAME(String s_NAME) {
        S_NAME = s_NAME;
    }

    public String getS_ADDRESS() {
        return S_ADDRESS;
    }

    public void setS_ADDRESS(String s_ADDRESS) {
        S_ADDRESS = s_ADDRESS;
    }

    public static Tuple2<String,String> toTuple(Result20 result20) {
        return Tuple2.of(result20.getS_NAME(),result20.getS_ADDRESS());
    }
}
