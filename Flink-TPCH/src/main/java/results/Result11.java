package results;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;

public class Result11 {

    public Integer PS_PARTKEY;
    public Float PART_VALUE;


    public Result11() {
    }

    public Integer getPS_PARTKEY() {
        return PS_PARTKEY;
    }

    public void setPS_PARTKEY(Integer PS_PARTKEY) {
        this.PS_PARTKEY = PS_PARTKEY;
    }

    public Float getPART_VALUE() {
        return PART_VALUE;
    }

    public void setPART_VALUE(Float PART_VALUE) {
        this.PART_VALUE = PART_VALUE;
    }

    public static Tuple2<Integer,Float> toTuple(Result11 result11) {
        return Tuple2.of(result11.getPS_PARTKEY(),result11.getPART_VALUE());
    }
}
