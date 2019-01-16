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
import results.Result5;
import results.Result6;
import results.Result7;
import tables.*;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q7 {

    public final static String TAG = "Q7";

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
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table customer = Customer.getTable(env,tEnv,dataAddress);

            //Query

            Table l_temp = lineitem.filter("L_SHIPDATE >= '1995-01-01' ").filter("L_SHIPDATE < '1996-12-31'");
            Table n_temp = nation.filter("N_NAME.like('JAPAN%') || N_NAME.like('INDIA%') ");

            Table s_n_l = n_temp.join(supplier).where("N_NATIONKEY == S_NATIONKEY")
                    .join(l_temp).where("S_SUPPKEY == L_SUPPKEY")
                    .select("N_NAME as SUPP_NATION,L_ORDERKEY,L_EXTENDEDPRICE,L_DISCOUNT,L_SHIPDATE");

            Table res = n_temp.join(customer).where("N_NATIONKEY == C_NATIONKEY")
                    .join(orders).where("C_CUSTKEY == O_CUSTKEY")
                    .select("N_NAME as CUST_NATION,O_ORDERKEY")
                    .join(s_n_l).where("O_ORDERKEY == L_ORDERKEY")
                    .distinct()
                    .filter("(SUPP_NATION.like('INDIA%') && CUST_NATION.like('JAPAN%')) || (SUPP_NATION.like('JAPAN%') && CUST_NATION.like('INDIA'))")
                    .select("SUPP_NATION,CUST_NATION,L_SHIPDATE,L_DISCOUNT,L_EXTENDEDPRICE")
                    .groupBy("SUPP_NATION,CUST_NATION,L_SHIPDATE")
                    .select("SUPP_NATION,CUST_NATION,L_SHIPDATE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE")
                    .orderBy("SUPP_NATION,CUST_NATION,L_SHIPDATE");

            //Convert Results
            DataSet<Result7> result = tEnv.toDataSet(res, Result7.class);

            //Print and Save Results
            result.map(p-> Result7.toTuple(p)).returns(new TypeHint<Tuple4<Float,String,String,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("7-QUERY");
    }
}
