package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result7;
import results.Result8;
import tables.*;
import utils.Util;
import utils.isJAPAN;

import java.io.PrintStream;
import java.util.HashMap;


public class Q8 {

    public final static String TAG = "Q8";

    public static void main(String[] args) throws Exception {


        //Save Logs to file
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

        env.getConfig().getRestartStrategy();

        tEnv.registerFunction("isJAPAN", new isJAPAN());

        try {

            Util.jobLog(TAG, Util.STATUS_START);

            Table lineitem = Lineitem.getTable(env,tEnv,dataAddress);
            Table orders = Orders.getTable(env,tEnv,dataAddress);
            Table nation = Nation.getTable(env,tEnv,dataAddress);
            Table region = Region.getTable(env,tEnv,dataAddress);
            Table supplier = Supplier.getTable(env,tEnv,dataAddress);
            Table customer = Customer.getTable(env,tEnv,dataAddress);
            Table part = Part.getTable(env,tEnv,dataAddress);

            //Query
            Table r_temp = region.filter("R_NAME == 'AMERICA' ");
            Table o_temp = orders.filter("O_ORDERDATE >= '1995-01-01' ").filter("O_ORDERDATE < '1996-12-31' ");
            Table p_temp = part.filter("P_TYPE == 'ECONOMY ANODIZED STEEL' ");
            Table n_s = nation.join(supplier).where("N_NATIONKEY == S_NATIONKEY");
            Table l_p = lineitem.join(p_temp).where("L_PARTKEY == P_PARTKEY");
            
            Table l_p_n_s = l_p
                .join(n_s)
                .where("L_SUPPKEY == S_SUPPKEY")
                .select("L_PARTKEY,L_SUPPKEY,L_ORDERKEY,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VOLUME");
            
            Table n_r = nation
                .join(r_temp)
                .where("N_REGIONKEY == R_REGIONKEY")
                .select("N_NATIONKEY,N_NAME");
            
            Table n_r_c = n_r
                .join(customer)
                .where("N_NATIONKEY == C_NATIONKEY")
                .select("C_CUSTKEY,N_NAME");
            
            Table n_r_c_o = n_r_c
                .join(o_temp)
                .where("C_CUSTKEY == O_CUSTKEY")
                .select("O_ORDERKEY,O_ORDERDATE,N_NAME");
            
            Table n_r_c_o_l = n_r_c_o
                .join(l_p_n_s)
                .where("O_ORDERKEY == L_ORDERKEY");
            
            Table res1 = n_r_c_o_l.select("O_ORDERDATE as O_YEAR ,VOLUME,isJAPAN(N_NAME,VOLUME) as JAPAN_VOLUME");
            Table res2 = res1
                .groupBy("O_YEAR")
                .select("O_YEAR as O_YEAR2,VOLUME.sum as TOTAL_VOLUME")
                .orderBy("O_YEAR2");
            
            Table res3 = res1
                .groupBy("O_YEAR")
                .select("O_YEAR as O_YEAR3,JAPAN_VOLUME.sum as TOTAL_JAPAN")
                .orderBy("O_YEAR3");
            
            Table res4 = res3
                .join(res2)
                .where("O_YEAR2 == O_YEAR3")
                .distinct()
                .select("O_YEAR3 as O_YEAR,(TOTAL_JAPAN/TOTAL_VOLUME) as MKT_SHARE");


            //Convert Results
            DataSet<Result8> result = tEnv.toDataSet(res4, Result8.class);

            //Print and Save Results
            result.map(p-> Result8.toTuple(p)).returns(new TypeHint<Tuple2<Float,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("8-QUERY");
    }
}
