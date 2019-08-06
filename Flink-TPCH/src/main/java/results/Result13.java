package results;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Result13 {

    public Long CUSTDIST;
    public Long C_COUNT;



    public Result13() {
    }

    public Long getCUSTDIST() {
        return CUSTDIST;
    }

    public void setCUSTDIST(Long CUSTDIST) {
        this.CUSTDIST = CUSTDIST;
    }

    public Long getC_COUNT() {
        return C_COUNT;
    }

    public void setC_COUNT(Long c_COUNT) {
        C_COUNT = c_COUNT;
    }

    public static Tuple2<Long,Long> toTuple(Result13 result13) {
        return Tuple2.of(result13.getCUSTDIST(),result13.getC_COUNT());
    }
}
