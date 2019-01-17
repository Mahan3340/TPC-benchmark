package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;

import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result1;
import results.TestResult;
import tables.Lineitem;
import tables.Nation;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;

public class Q1 {

    public final static String TAG = "Q1";
    public static void main(String[] args) throws Exception {

        HashMap<String,String> addresses = Util.readAddress(TAG);
        String dataAddress = addresses.get("data");
        String outputAddress = addresses.get("out");

        if(addresses == null)
        {
            return;
        }

        //ENV Definations
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        try {

            Util.jobLog(TAG,Util.STATUS_START);


            Table lineitem = Lineitem.getTable(env,tEnv,dataAddress);

            //Query
            Table resultTable = lineitem
                .filter("L_SHIPDATE <= '1998-09-02'")
                .groupBy("L_RETURNFLAG,L_LINESTATUS")
                .select("L_RETURNFLAG,L_LINESTATUS,L_QUANTITY.sum as SUM_QTY,(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as SUM_DISC_PRICE," +
                    "L_EXTENDEDPRICE.sum as SUM_BASE_PRICE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)).sum as SUM_CHARGE,L_QUANTITY.avg as AVG_QTY,L_EXTENDEDPRICE.avg as AVG_PRICE,L_DISCOUNT.avg as AVG_DISC")
                .orderBy("L_RETURNFLAG,L_LINESTATUS");

            //Convert Results
            DataSet<Result1> result = tEnv.toDataSet(resultTable, Result1.class);

            //Print and Save Results
            result.map(p -> Result1.toTuple(p)).returns(new TypeHint<Tuple9<String,String,Float,Float,Float,Float,Float,Float,Float>>() {})
                    .writeAsCsv(outputAddress + TAG +"-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG,Util.STATUS_DONE);

        }catch (Exception e)
        {
            e.printStackTrace();
            Util.jobLog(TAG,Util.STATUS_FAIELD);
        }

        env.execute("1-QUERY");
    }
}
