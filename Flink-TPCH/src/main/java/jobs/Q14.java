package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result13;
import results.Result14;
import tables.Customer;
import tables.Lineitem;
import tables.Orders;
import tables.Part;
import utils.Promo;
import utils.Util;
import utils.isJAPAN;

import java.io.PrintStream;
import java.util.HashMap;


public class Q14 {

    public final static String TAG = "Q14";

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

        tEnv.registerFunction("promo", new Promo());

        try {

            Util.jobLog(TAG, Util.STATUS_START);

            Table lineitem = Lineitem.getTable(env,tEnv,dataAddress);
            Table part = Part.getTable(env,tEnv,dataAddress);


            Table l_temp = lineitem.filter("L_SHIPDATE >='1996-12-01'  && L_SHIPDATE < '1997-01-01'");
            Table l_p = part.join(l_temp).where("P_PARTKEY == L_PARTKEY");
            Table res1 = l_p.select("P_TYPE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VALUE");
            
            Float totalSum = tEnv
                .toDataSet(res1.select("VALUE.sum as TOTAL_VALUE"),Float.class)
                .collect()
                
                .get(0);
            Table res2 = res1
                .select("(promo(P_TYPE,VALUE)).sum*100/totalSum as PROMO_REVENUE"
                        .replace("totalSum",totalSum + ""));


            //Convert Results
            DataSet<Result14> result = tEnv.toDataSet(res2,Result14.class);

            //Print and Save Results
            result.map(p-> Result14.toTuple(p)).returns(new TypeHint<Tuple1<Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("14-QUERY");
    }
}
