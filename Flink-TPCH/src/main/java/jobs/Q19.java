package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result18;
import results.Result19;
import tables.Customer;
import tables.Lineitem;
import tables.Orders;
import tables.Part;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q19 {

    public final static String TAG = "Q19";

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

            Table p_l = part.join(lineitem).where("P_PARTKEY == L_PARTKEY")
                    .filter(" ( L_SHIPMODE.like('%AIR%') || L_SHIPMODE.like('%AIR REG%') ) && (L_SHIPINSTRUCT.like('%DELIVER IN PERSON%'))");

            Table p_l_2 = p_l.filter(
                    "( (P_BRAND.like('%Brand#54%')) && P_CONTAINER.in('SM CASE','SM BOX','SM PACK','SM PKG')  && (L_QUANTITY >= 8) && (L_QUANTITY <= 18) &&  (P_SIZE >= 1) && (P_SIZE <= 10) ) " +
                            "|| ( (P_BRAND.like('%Brand#22%')) && P_CONTAINER.in('MED BAG','MED BOX','MED PKG','MED PACK')  && (L_QUANTITY >= 3) && (L_QUANTITY <= 23) &&  (P_SIZE >= 1) && (P_SIZE <= 5) )  " +
                            "|| ( (P_BRAND.like('%Brand#51%')) && P_CONTAINER.in('LG CASE','LG BOX','LG PACK','LG PKG')  && (L_QUANTITY >= 22) && (L_QUANTITY <= 32) &&  (P_SIZE >= 1) && (P_SIZE <= 15) )" )
                    .select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VOLUME").select("VOLUME.sum as REVENUE");
            //Convert Results
            DataSet<Result19> result = tEnv.toDataSet(p_l_2,Result19.class);

            //Print and Save Results
            result.map(p-> Result19.toTuple(p)).returns(new TypeHint<Tuple1<Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("19-QUERY");
    }
}
