package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result3;
import results.Result4;
import results.Result5;
import tables.Customer;
import tables.Lineitem;
import tables.Nation;
import tables.Orders;
import utils.Util;

import javax.sound.sampled.Line;
import java.io.PrintStream;
import java.util.HashMap;


public class Q4 {

    public final static String TAG = "Q4";

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
            Table orders = Orders.getTable(env,tEnv,dataAddress);

            //Query

            Table l_temp = lineitem.filter("L_COMMITDATE < L_RECEIPTDATE");
            Table o_temp = orders.filter("O_ORDERDATE >= '1995-01-01' ").filter("O_ORDERDATE < '1995-04-01' ");
            Table l_o = l_temp.join(o_temp).where("L_ORDERKEY == O_ORDERKEY")
                    .groupBy("O_ORDERPRIORITY")
                    .select("O_ORDERPRIORITY,O_ORDERPRIORITY.count as ORDER_COUNT")
                    .orderBy("O_ORDERPRIORITY");

            //Convert Results
            DataSet<Result4> result = tEnv.toDataSet(l_o, Result4.class);

            //Print and Save Results
            result.map(p-> Result4.toTuple(p)).returns(new TypeHint<Tuple2<Long,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("4-QUERY");
    }
}
