package utils;

import org.apache.flink.table.functions.ScalarFunction;

public class isHigh extends ScalarFunction {

    public int eval(String x)
    {
        if(x.contains("2-HIGH") || x.contains("1-URGENT") )
        {
            return 1;
        }
        return 0;
    }
}
