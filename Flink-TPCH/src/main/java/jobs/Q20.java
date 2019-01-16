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
import results.Result20;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q20 {

    public final static String TAG = "Q20";

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
            Table part = Part.getTable(env,tEnv,dataAddress);
            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table partsupp = PartSupp.getTable(env,tEnv,dataAddress);

            //Query
            Table l_temp = lineitem.filter("L_SHIPDATE >= '1993-01-01' && L_SHIPDATE < '1994-01-01' ")
                    .groupBy("L_PARTKEY,L_SUPPKEY").select("(L_QUANTITY*0.5).sum as SUM_QUANTITY,L_SUPPKEY,L_PARTKEY");
            Table n_temp = nation.filter("N_NAME.like('%CANADA%')");
            Table n_s = supplier.select("S_SUPPKEY,S_NAME,S_NATIONKEY,S_ADDRESS").join(n_temp).where("S_NATIONKEY == N_NATIONKEY");
            Table p_temp = part.filter("P_NAME.like('forest%')").select("P_PARTKEY");
            Table p_ps = p_temp.join(partsupp).where("P_PARTKEY == PS_PARTKEY");
            Table p_ps_l = p_ps.join(l_temp).where("L_SUPPKEY == PS_SUPPKEY && L_PARTKEY == PS_PARTKEY")
                    .filter("PS_AVAILQTY > SUM_QUANTITY").select("PS_SUPPKEY");
            Table p_ps_l_n_s = p_ps_l.join(n_s).where("S_SUPPKEY == PS_SUPPKEY")
                    .select("S_NAME,S_ADDRESS").orderBy("S_NAME");

            //Convert Results
            DataSet<Result20> result = tEnv.toDataSet(p_ps_l_n_s,Result20.class);

            //Print and Save Results
            result.map(p-> Result20.toTuple(p)).returns(new TypeHint<Tuple2<String,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("20-QUERY");
    }
}
