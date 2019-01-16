package jobs;

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
import results.Result12;
import results.Result13;
import tables.*;
import utils.Util;
import utils.isHigh;
import utils.isLow;

import java.io.PrintStream;
import java.util.HashMap;


public class Q13 {

    public final static String TAG = "Q13";

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
            Table customer = Customer.getTable(env,tEnv,dataAddress);

            Table o_temp = orders.filter("!O_COMMENT.like('%pending%deposits%')");
            Table c_o = customer.leftOuterJoin(o_temp).where("C_CUSTKEY == O_CUSTKEY");
            Table res1 = c_o.groupBy("O_ORDERKEY").select("O_ORDERKEY.count as C_COUNT,O_ORDERKEY");
            c_o = c_o.select("O_CUSTKEY,O_ORDERKEY as O_ORDERKEY2");
            Table res2 = res1.join(c_o).where("O_ORDERKEY2 == O_ORDERKEY")
                    .select("C_COUNT,O_CUSTKEY,O_ORDERKEY")
                    .groupBy("C_COUNT")
                    .select("O_CUSTKEY.count as CUSTDIST,C_COUNT")
                    .orderBy("CUSTDIST.desc,C_COUNT.desc");

            //Convert Results
            DataSet<Result13> result = tEnv.toDataSet(res2, Result13.class);
            //Print and Save Results
            result.map(p-> Result13.toTuple(p)).returns(new TypeHint<Tuple2<Long,Long>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("13-QUERY");
    }
}
