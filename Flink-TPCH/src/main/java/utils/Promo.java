package utils;


public class Promo extends org.apache.flink.table.functions.ScalarFunction {
    public float eval(String x, Float y) {
        if (x.startsWith("PROMO")) {
            return y;

        }
        return 0;
    }
}
