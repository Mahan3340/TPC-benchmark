package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result15;
import tables.Lineitem;
import tables.Part;
import tables.Supplier;
import utils.Promo;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q15 {

    public final static String TAG = "Q15";

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
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);

            Table l_temp = lineitem.filter("L_SHIPDATE >= '1997-07-01' && L_SHIPDATE < '1997-10-01'").select("L_SUPPKEY,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VALUE");
            Table revenue = l_temp.groupBy("L_SUPPKEY").select("VALUE.sum as TOTAL,L_SUPPKEY");
            Float max_total = tEnv.toDataSet(revenue.select("TOTAL.max as MAX_TOTAL"),Float.class).collect().get(0);
            Table res = revenue.filter("TOTAL == max_total".replace("max_total",max_total+""))
                    .join(supplier).where("S_SUPPKEY == L_SUPPKEY").select("S_SUPPKEY,S_NAME,S_ADDRESS,S_PHONE,TOTAL")
                    .orderBy("S_SUPPKEY");

            //Convert Results
            DataSet<Result15> result = tEnv.toDataSet(res,Result15.class);

            //Print and Save Results
            result.map(p-> Result15.toTuple(p)).returns(new TypeHint<Tuple5<Integer,String,String,String,Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("15-QUERY");
    }
}
