package jobs;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result11;
import results.Result12;
import tables.*;
import utils.Util;
import utils.isHigh;
import utils.isJAPAN;
import utils.isLow;

import java.io.PrintStream;
import java.util.HashMap;


public class Q12 {

    public final static String TAG = "Q12";

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

        tEnv.registerFunction("isHigh", new isHigh());
        tEnv.registerFunction("isLow", new isLow());

        try {

            Util.jobLog(TAG, Util.STATUS_START);


            Table lineitem = Lineitem.getTable(env,tEnv,dataAddress);
            Table orders = Orders.getTable(env,tEnv,dataAddress);

            //Query
            Table l_temp = lineitem.filter("(L_SHIPMODE = 'MAIL' || L_SHIPMODE == 'SHIP') && L_COMMITDATE < L_RECEIPTDATE && " +
                    "L_SHIPDATE < L_COMMITDATE && L_RECEIPTDATE >= '1997-01-01' && L_RECEIPTDATE <= '1998-01-01' ");
            Table l_o = l_temp
                .join(orders)
                .where("L_ORDERKEY == O_ORDERKEY")
                .select("L_SHIPMODE,O_ORDERPRIORITY");
            Table res = l_o
                    .groupBy("L_SHIPMODE")
                    .select("L_SHIPMODE,isHigh(O_ORDERPRIORITY).sum as HIGH_LINE_COUNT,isLow(O_ORDERPRIORITY).sum as LOW_LINE_COUNT")
                    .orderBy("L_SHIPMODE");

            //Convert Results
            DataSet<Result12> result = tEnv.toDataSet(res, Result12.class);
            //Print and Save Results
            result.map(p-> Result12.toTuple(p)).returns(new TypeHint<Tuple3<String,Integer,Integer>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("12-QUERY");
    }
}
