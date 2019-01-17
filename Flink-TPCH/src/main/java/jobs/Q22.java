package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result22;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q22 {

    public final static String TAG = "Q22";

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

            Table customer = Customer.getTable(env,tEnv,dataAddress);
            Table orders = Orders.getTable(env,tEnv,dataAddress);

            //Query

           // codes = ["20", "40", "22", "30", "39", "42", "21"]
            Table c_temp = customer.select("C_ACCTBAL,C_CUSTKEY,C_PHONE.substring(1,2) as COUNTRY_CODE ");
            c_temp = c_temp.filter("COUNTRY_CODE.in('20', '40', '22', '30', '39', '42', '21')");
            Table c_avg = c_temp.filter("C_ACCTBAL > 0.0 ").select("C_ACCTBAL.avg as AVG_ACCTBAL");
            Table o_c = orders.select("O_CUSTKEY").rightOuterJoin(c_temp,"O_CUSTKEY == C_CUSTKEY");
            Table o_c_cavg = o_c
                .join(c_avg)
                .filter("C_ACCTBAL > AVG_ACCTBAL")
                .groupBy("COUNTRY_CODE")
                .select("C_ACCTBAL.count as ACCTBAL_COUNT,C_ACCTBAL.sum as ACCTBAL_SUM,COUNTRY_CODE")
                .orderBy("COUNTRY_CODE");

            //Convert Results
            DataSet<Result22> result = tEnv.toDataSet(o_c_cavg,Result22.class);

            //Print and Save Results
            result.map(p-> Result22.toTuple(p)).returns(new TypeHint<Tuple3<Long,Float,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("22-QUERY");
    }
}
