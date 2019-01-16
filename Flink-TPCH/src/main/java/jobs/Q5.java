package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result4;
import results.Result5;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q5 {

    public final static String TAG = "Q5";

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
            Table region = Region.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table customer = Customer.getTable(env,tEnv,dataAddress);

            //Query
            Table r_temp = region.filter("R_NAME == 'MIDDLE EAST' ");
            Table o_temp = orders.filter("O_ORDERDATE >= '1994-01-01' ").filter("O_ORDERDATE < '1995-01-01' ");
            Table c_o = customer.join(o_temp).where("C_CUSTKEY == O_CUSTKEY").select("O_ORDERKEY");
            Table r_n = r_temp.join(nation).where("R_REGIONKEY == N_REGIONKEY");
            Table r_n_s = r_n.join(supplier).where("N_NATIONKEY == S_NATIONKEY");
            Table r_n_s_l = r_n_s.join(lineitem).where("S_SUPPKEY == L_SUPPKEY")
                    .select("N_NAME,L_EXTENDEDPRICE,L_DISCOUNT,L_ORDERKEY");
            Table r_n_s_l_c_o = r_n_s_l.join(c_o).where("L_ORDERKEY == O_ORDERKEY");
            Table res = r_n_s_l_c_o.groupBy("N_NAME")
                    .select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE")
                    .orderBy("REVENUE.desc");

            //Convert Results
            DataSet<Result5> result = tEnv.toDataSet(res, Result5.class);

            //Print and Save Results
            result.map(p->Result5.toTuple(p)).returns(new TypeHint<Tuple1<Float>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("5-QUERY");
    }
}
