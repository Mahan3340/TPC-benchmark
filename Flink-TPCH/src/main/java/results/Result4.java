package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class Result4 {

    public Long ORDER_COUNT;
    public String O_ORDERPRIORITY;

    public Result4() {}

    public Long getORDER_COUNT() {
        return ORDER_COUNT;
    }

    public void setORDER_COUNT(Long ORDER_COUNT) {
        this.ORDER_COUNT = ORDER_COUNT;
    }

    public String getO_ORDERPRIORITY() {
        return O_ORDERPRIORITY;
    }

    public void setO_ORDERPRIORITY(String o_ORDERPRIORITY) {
        O_ORDERPRIORITY = o_ORDERPRIORITY;
    }

    public static Tuple2<Long,String> toTuple(Result4 result4) {
        return Tuple2.of(result4.getORDER_COUNT(),result4.getO_ORDERPRIORITY());
    }
}
