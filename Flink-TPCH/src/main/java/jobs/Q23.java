package jobs;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result22;
import results.Result23;
import tables.Customer;
import tables.Orders;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q23 {

    public final static String TAG = "Q23";

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

            Table orders = Orders.getTable(env,tEnv,dataAddress);

            //Query

            Table o_temp = orders.filter("O_ORDERDATE >= '1994-01-01' ").filter("O_ORDERDATE < '1994-11-01' ").select("O_ORDERDATE")
                    .orderBy("O_ORDERDATE")
                    .fetch(10);


            //Convert Results
            DataSet<Result23> result = tEnv.toDataSet(o_temp,Result23.class);
            result.map(p-> Result23.toTuple(p)).returns(new TypeHint<Tuple1<String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|");
            //Print and Save Results
            //result.print();

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("23-QUERY");
    }
}
