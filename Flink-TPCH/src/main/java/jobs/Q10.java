package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result10;
import results.Result9;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q10 {

    public final static String TAG = "Q10";

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
            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table customer = Customer.getTable(env,tEnv,dataAddress);

            //Query
            Table l_temp = lineitem.filter("L_RETURNFLAG == 'R'");
            Table o_temp = orders.filter("O_ORDERDATE >= '1994-01-01' ").filter("O_ORDERDATE < '1994-11-01' ");
            Table o_c = o_temp.join(customer).where("C_CUSTKEY == O_CUSTKEY");
            Table o_c_n = o_c.join(nation).where("C_NATIONKEY == N_NATIONKEY");
            Table o_c_n_l = o_c_n.join(l_temp).where("L_ORDERKEY == O_ORDERKEY");
            Table res = o_c_n_l
                .select("C_CUSTKEY,C_NAME,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VOLUME,C_ACCTBAL,N_NAME,C_ADDRESS,C_PHONE,C_COMMENT")
                .groupBy("C_CUSTKEY,C_NAME,C_ACCTBAL,N_NAME,C_ADDRESS,C_COMMENT,C_PHONE")
                .select("VOLUME.sum as REVENUE,C_CUSTKEY,C_NAME,C_ACCTBAL,N_NAME,C_ADDRESS,C_COMMENT,C_PHONE");

            //Convert Results
            DataSet<Result10> result = tEnv.toDataSet(res, Result10.class);

            //Print and Save Results
            result.map(p-> Result10.toTuple(p)).returns(new TypeHint<Tuple8<Integer,String,Float,Float,String,String,String,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("10-QUERY");
    }
}
