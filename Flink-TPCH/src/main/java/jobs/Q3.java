package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;

import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result2;
import results.Result3;
import results.Result4;
import tables.*;
import utils.Util;

import javax.sound.sampled.Line;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Scanner;


public class Q3 {

    public final static String TAG = "Q3";

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
            Table customer = Customer.getTable(env,tEnv,dataAddress);
            Table orders = Orders.getTable(env,tEnv,dataAddress);

            //Query

            Table c_temp = customer.filter("C_MKTSEGMENT == 'AUTOMOBILE'");
            Table l_temp = lineitem.filter("L_SHIPDATE > '1995-03-13'");
            Table o_temp = orders.filter("O_ORDERDATE < '1995-03-13' ");
            Table l_o = l_temp.join(o_temp).where("L_ORDERKEY == O_ORDERKEY");
            Table c_o_l = c_temp.join(l_o).where("C_CUSTKEY == O_CUSTKEY");
            Table res = c_o_l.select("O_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY").join(l_temp).where("O_ORDERKEY == L_ORDERKEY")
                    .select("O_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY,L_ORDERKEY,L_EXTENDEDPRICE,L_DISCOUNT");
            Table res2 = res.groupBy("L_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY").select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE,O_ORDERDATE")
                    .orderBy("O_ORDERDATE,REVENUE.desc").fetch(10);

            //Convert Results
            DataSet<Result3> result = tEnv.toDataSet(res2, Result3.class);

            //Print and Save Results
            result.map(p-> Result3.toTuple(p)).returns(new TypeHint<Tuple2<String,Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("3-QUERY");
    }
}
