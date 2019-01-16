package results;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.expressions.In;

public class Result18 {

   public Float L_QUANTITY_SUM;
   public String O_ORDERDATE;
   public Float O_TOTALPRICE;
   public Integer O_ORDERKEY;
   public Integer C_CUSTKEY;
   public String C_NAME;



    public Result18() {
    }

    public Float getL_QUANTITY_SUM() {
        return L_QUANTITY_SUM;
    }

    public void setL_QUANTITY_SUM(Float l_QUANTITY_SUM) {
        L_QUANTITY_SUM = l_QUANTITY_SUM;
    }

    public String getO_ORDERDATE() {
        return O_ORDERDATE;
    }

    public void setO_ORDERDATE(String o_ORDERDATE) {
        O_ORDERDATE = o_ORDERDATE;
    }

    public Float getO_TOTALPRICE() {
        return O_TOTALPRICE;
    }

    public void setO_TOTALPRICE(Float o_TOTALPRICE) {
        O_TOTALPRICE = o_TOTALPRICE;
    }

    public Integer getO_ORDERKEY() {
        return O_ORDERKEY;
    }

    public void setO_ORDERKEY(Integer o_ORDERKEY) {
        O_ORDERKEY = o_ORDERKEY;
    }

    public Integer getC_CUSTKEY() {
        return C_CUSTKEY;
    }

    public void setC_CUSTKEY(Integer c_CUSTKEY) {
        C_CUSTKEY = c_CUSTKEY;
    }

    public String getC_NAME() {
        return C_NAME;
    }

    public void setC_NAME(String c_NAME) {
        C_NAME = c_NAME;
    }

    public static Tuple6<Float,String,Float,Integer,Integer,String> toTuple(Result18 result18) {
        return Tuple6.of(result18.getL_QUANTITY_SUM(),result18.getO_ORDERDATE(),result18.getO_TOTALPRICE(),result18.getO_ORDERKEY(),
                result18.getC_CUSTKEY(),result18.getC_NAME());
    }
}
