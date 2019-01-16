package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result10;
import results.Result11;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q11 {

    public final static String TAG = "Q11";

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

            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table partsupp = PartSupp.getTable(env,tEnv,dataAddress);

            //Query
            Table n_temp = nation.filter("N_NAME.like('%ARGENTINA%')");
            Table n_s = n_temp.join(supplier).where("N_NATIONKEY == S_NATIONKEY").select("S_SUPPKEY");
            Table n_s_ps = n_s.join(partsupp).where("S_SUPPKEY == PS_SUPPKEY")
                    .select("PS_PARTKEY,(PS_SUPPLYCOST*PS_AVAILQTY) as VALUE");
            Table sum = n_s_ps.select("VALUE.sum as TOTAL_VALUE");
            Table res1 = n_s_ps
                .groupBy("PS_PARTKEY")
                .select("VALUE.sum as PART_VALUE,PS_PARTKEY");
            Table res2 = res1
                .join(sum)
                .filter("PART_VALUE > (TOTAL_VALUE*0.0001)")
                .orderBy("PART_VALUE.desc")
                .select("PS_PARTKEY,PART_VALUE");

            //Convert Results
            DataSet<Result11> result = tEnv.toDataSet(res2, Result11.class);

            //Print and Save Results
            result.map(p-> Result11.toTuple(p)).returns(new TypeHint<Tuple2<Integer,Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("11-QUERY");
    }
}
