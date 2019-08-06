package utils;

import org.apache.flink.table.functions.ScalarFunction;

public class isJAPAN extends ScalarFunction {

  public float eval(String x , Float y)
  {
      if(x.contains("JAPAN"))
      {
          return y;
      }
      return 0;

  }

}