package results;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class Result8 {

    public Float MKT_SHARE;
    public String O_YEAR;


    public Result8() {}

    public Float getMKT_SHARE() {
        return MKT_SHARE;
    }

    public void setMKT_SHARE(Float MKT_SHARE) {
        this.MKT_SHARE = MKT_SHARE;
    }

    public String getO_YEAR() {
        return O_YEAR;
    }

    public void setO_YEAR(String o_YEAR) {
        O_YEAR = o_YEAR;
    }

    public static Tuple2<Float,String> toTuple(Result8 result8) {
        return Tuple2.of(result8.getMKT_SHARE(),result8.getO_YEAR());
    }

}
