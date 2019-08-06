package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result5;
import results.Result6;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q6 {

    public final static String TAG = "Q6";

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

           Util.jobLog(TAG, Util.STATUS_START);

            Table lineitem = Lineitem.getTable(env,tEnv,dataAddress);

            //Query
            Table l_temp = lineitem
                    .filter("L_SHIPDATE >= '1994-01-01' ")
                    .filter("L_SHIPDATE < '1995-01-01'")
                    .filter("L_DISCOUNT >= 0.05")
                    .filter("L_DISCOUNT <= 0.07")
                    .filter("L_QUANTITY < 24")
                    .select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE");

            //Convert Results
            DataSet<Result6> result = tEnv.toDataSet(l_temp, Result6.class);

            //Print and Save Results
            result.map(p->Result6.toTuple(p)).returns(new TypeHint<Tuple1<Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("6-QUERY");
    }
}
