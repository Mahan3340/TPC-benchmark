package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

public class Result23 {


    public String O_ORDERDATE;

    public Result23() {
    }

    public String getO_ORDERDATE() {
        return O_ORDERDATE;
    }

    public void setO_ORDERDATE(String o_ORDERDATE) {
        O_ORDERDATE = o_ORDERDATE;
    }

    public static Tuple1<String> toTuple(Result23 result23) {
        return Tuple1.of(result23.getO_ORDERDATE());
    }
}
