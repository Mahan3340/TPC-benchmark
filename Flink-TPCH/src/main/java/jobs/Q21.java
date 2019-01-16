package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result21;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q21 {

    public final static String TAG = "Q21";

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
            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table orders = Orders.getTable(env,tEnv,dataAddress);

            //Query

            Table s_temp = supplier.select("S_SUPPKEY,S_NATIONKEY,S_NAME");
            Table l_temp = lineitem.select("L_SUPPKEY,L_ORDERKEY,L_RECEIPTDATE,L_COMMITDATE");
            Table l_temp2 = l_temp.filter("L_RECEIPTDATE > L_COMMITDATE");
            Table o_temp = orders.select("O_ORDERKEY,O_ORDERSTATUS").filter("O_ORDERSTATUS = 'F' ");
            Table n_temp = nation.filter("N_NAME.like('%EGYPT%') ");

            Table line1 = l_temp
                .groupBy("L_ORDERKEY")
                .select("L_ORDERKEY,L_SUPPKEY.count as SUPPKEY_COUNT,L_SUPPKEY.max as SUPPKEY_MAX ").select("L_ORDERKEY as KEY,SUPPKEY_COUNT,SUPPKEY_MAX");
            Table line2 = l_temp
                .groupBy("L_ORDERKEY")
                .select("L_ORDERKEY,L_SUPPKEY.count as SUPPKEY_COUNT,L_SUPPKEY.max as SUPPKEY_MAX ").select("L_ORDERKEY as KEY,SUPPKEY_COUNT,SUPPKEY_MAX");


            Table n_s = n_temp.join(s_temp).where("S_NATIONKEY == N_NATIONKEY");
            Table n_s_l = n_s.join(l_temp2).where("S_SUPPKEY == L_SUPPKEY");
            Table n_s_l_o = n_s_l.join(o_temp).where("O_ORDERKEY == L_ORDERKEY");
            
            Table n_s_l_o_line1 = n_s_l_o.
                join(line1)
                .where("KEY == L_ORDERKEY")
                .filter(" ( SUPPKEY_COUNT>1 || SUPPKEY_COUNT == 1 ) && (L_SUPPKEY == SUPPKEY_MAX) ")
                .select("S_NAME,L_ORDERKEY,L_SUPPKEY");

            Table n_s_l_o_line1_line2 = n_s_l_o_line1
                .leftOuterJoin(line2)
                .where("L_ORDERKEY == KEY")
                .select("S_NAME, L_ORDERKEY, L_SUPPKEY,SUPPKEY_COUNT,SUPPKEY_MAX")
                .filter("(SUPPKEY_COUNT == 1) && (L_SUPPKEY == SUPPKEY_MAX)" )
                .groupBy("S_NAME")
                .select("S_NAME,L_SUPPKEY.count as NUM_WAIT")
                .orderBy("NUM_WAIT.desc,S_NAME");


            //Convert Results
            DataSet<Result21> result = tEnv.toDataSet(n_s_l_o_line1_line2,Result21.class);

            //Print and Save Results
            result.map(p-> Result21.toTuple(p)).returns(new TypeHint<Tuple2<String,Long>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("21-QUERY");
    }
}
