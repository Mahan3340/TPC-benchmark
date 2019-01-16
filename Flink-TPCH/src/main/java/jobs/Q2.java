package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;

import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result1;
import results.Result2;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q2 {

    public final static String TAG = "Q2";
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

            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table region = Region.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table part = Part.getTable(env,tEnv,dataAddress);
            Table partsupp = PartSupp.getTable(env,tEnv,dataAddress);

            //Query
            Table r_temp = region.filter("R_NAME == 'ASIA' ");
            Table n_r = nation.join(r_temp).where("N_REGIONKEY == R_REGIONKEY");
            Table p_temp = part.filter("P_SIZE == 30").filter("P_TYPE.like('%STEEL%')");
            Float min_supp_cost = tEnv.toDataSet(partsupp.select("(PS_SUPPLYCOST).min as min_supp_cost"),Float.class).collect().get(0);
            String query = "PS_SUPPLYCOST == min".replace("min",min_supp_cost+"");
            Table ps_temp = partsupp.filter(query);
            Table n_r_s = n_r.join(supplier).where("N_NATIONKEY == S_NATIONKEY");
            Table n_r_s_ps = n_r_s.join(ps_temp).where("PS_SUPPKEY == S_SUPPKEY");
            Table n_r_s_p_ps = n_r_s_ps.join(p_temp).where("P_PARTKEY == PS_PARTKEY").select("S_ACCTBAL,S_NAME,N_NAME,P_PARTKEY,P_MFGR,S_ADDRESS,S_PHONE" +
                    ",S_COMMENT").orderBy("S_ACCTBAL.desc,N_NAME,S_NAME,P_PARTKEY").fetch(100);


            //Convert Results
            DataSet<Result2> result = tEnv.toDataSet(n_r_s_p_ps, Result2.class);

            //Print and Save Results
            result.map(p -> Result2.toTuple(p)).returns(new TypeHint<Tuple8<Float,String,String,Integer,String,String,String,String>>() {})
                    .writeAsCsv(outputAddress + TAG +"-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG,Util.STATUS_DONE);

        }catch (Exception e)
        {
            e.printStackTrace();
            Util.jobLog(TAG,Util.STATUS_FAIELD);
        }
         env.execute("2-QUERY");
         
    }
}
