package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result17;
import tables.Lineitem;
import tables.Part;
import tables.PartSupp;
import tables.Supplier;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q17 {

    public final static String TAG = "Q17";

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

            Table l_temp = lineitem.select("L_PARTKEY,L_QUANTITY,L_EXTENDEDPRICE");
            Table p_temp = part.filter("P_BRAND.like('%Brand#44%') && P_CONTAINER.like('%WRAP PKG%') ");
            Table p_l = p_temp.leftOuterJoin(l_temp).where("P_PARTKEY == L_PARTKEY");
            Table p_temp2 = p_l.groupBy("P_PARTKEY").select("(L_QUANTITY*0.2).avg as AVG_QUANTITY,P_PARTKEY as KEY");
            p_temp2 = p_temp2.join(p_l).where("KEY == P_PARTKEY").select("AVG_QUANTITY,KEY,L_QUANTITY,L_EXTENDEDPRICE");
            Table p_p = p_temp
                .join(p_temp2)
                .where("KEY == P_PARTKEY")
                .filter("L_QUANTITY < AVG_QUANTITY")
                .select("(L_EXTENDEDPRICE/0.7) as AVG_YEARLY");


            //Convert Results
            DataSet<Result17> result = tEnv.toDataSet(p_p,Result17.class);

            //Print and Save Results
            result.map(p-> Result17.toTuple(p)).returns(new TypeHint<Tuple1<Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("17-QUERY");
    }
}
