package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result16;
import tables.Lineitem;
import tables.Part;
import tables.PartSupp;
import tables.Supplier;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q16 {

    public final static String TAG = "Q16";

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
            
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table partsupp = PartSupp.getTable(env,tEnv,dataAddress);
            Table part = Part.getTable(env,tEnv,dataAddress);

            Table p_temp = part
                .filter("P_BRAND != 'Brand#31'&& !P_TYPE.like('LARGE PLATED%') && P_SIZE.in(48, 19, 12, 4, 41, 7, 21, 39)")
                .select("P_PARTKEY,P_BRAND,P_TYPE,P_SIZE");
            Table s_ps = supplier
                .filter("S_COMMENT.like('%Customer%Complaints%')")
                .join(partsupp)
                .where("S_SUPPKEY == PS_SUPPKEY");
            
            Table s_p_ps = s_ps
                .join(p_temp)
                .where("PS_PARTKEY == P_PARTKEY")
                .groupBy("P_BRAND,P_TYPE,P_SIZE")
                .select(" PS_SUPPKEY.count as SUPPLIER_COUNT,P_BRAND,P_TYPE,P_SIZE")
                .orderBy("SUPPLIER_COUNT.desc,P_BRAND,P_TYPE,P_SIZE");


            //Convert Results
            DataSet<Result16> result = tEnv.toDataSet(s_p_ps,Result16.class);

            //Print and Save Results
            result.map(p-> Result16.toTuple(p)).returns(new TypeHint<Tuple4<Long,String,String,Integer>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("16-QUERY");
    }
}
