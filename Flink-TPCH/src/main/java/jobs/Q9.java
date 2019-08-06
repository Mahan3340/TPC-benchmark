package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result9;
import tables.*;
import utils.Util;


import java.io.PrintStream;
import java.util.HashMap;


public class Q9 {

    public final static String TAG = "Q9";

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

            //Get Tables
            Table lineitem = Lineitem.getTable(env,tEnv,dataAddress);
            Table orders = Orders.getTable(env,tEnv,dataAddress);
            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table part = Part.getTable(env,tEnv,dataAddress);
            Table partsupp = PartSupp.getTable(env,tEnv,dataAddress);

            //Query
            Table p_temp = part.filter("P_NAME.like('%dim%')");
            Table l_p = lineitem.join(p_temp).where("P_PARTKEY == L_PARTKEY");
            Table n_s = nation.join(supplier).where("N_NATIONKEY == S_NATIONKEY");
            Table l_p_s_ = l_p.join(n_s).where("L_SUPPKEY == S_SUPPKEY");
            Table l_p_s_ps = l_p_s_.join(partsupp).where("L_SUPPKEY == PS_SUPPKEY");
            Table l_p_s_ps_o = l_p_s_ps.join(orders).where("L_ORDERKEY == O_ORDERKEY");
            Table profit = l_p_s_ps_o.select("N_NAME,O_ORDERDATE as O_YEAR,(L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY) as AMOUNT");
            
            Table res = profit
                .groupBy("N_NAME,O_YEAR")
                .select("AMOUNT.sum as SUM_PROFIT,O_YEAR,N_NAME")
                .orderBy("N_NAME,O_YEAR.desc");

            //Convert Results
            DataSet<Result9> result = tEnv.toDataSet(res, Result9.class);

            //Convert Results to CSV and save to HDFS
            result.map(p-> Result9.toTuple(p)).returns(new TypeHint<Tuple3<Float,String,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("9-QUERY");
    }
}
