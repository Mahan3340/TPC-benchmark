package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.Result18;
import tables.Customer;
import tables.Lineitem;
import tables.Orders;
import tables.Part;
import utils.Util;

import java.io.PrintStream;
import java.util.HashMap;


public class Q18 {

    public final static String TAG = "Q18";

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

            Table l_temp = lineitem.groupBy("L_ORDERKEY")
                    .select("L_QUANTITY.sum as SUM_QUANTITY,L_ORDERKEY as KEY")
                    .filter("SUM_QUANTITY > 300");
            
            Table l_o = l_temp.join(orders).where("KEY == O_ORDERKEY");
            l_o = l_o.join(lineitem).where("L_ORDERKEY == KEY");
            
            Table l_o_c = l_o.join(customer).where("C_CUSTKEY == O_CUSTKEY")
                    .select("L_QUANTITY,C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,O_CUSTKEY")
                    .groupBy("C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE")
                    .select("L_QUANTITY.sum as L_QUANTITY_SUM,O_ORDERDATE,O_TOTALPRICE,O_ORDERKEY,C_CUSTKEY,C_NAME")
                    .orderBy("O_TOTALPRICE.desc,O_ORDERDATE")
                    .fetch(100);

            //Convert Results
            DataSet<Result18> result = tEnv.toDataSet(l_o_c,Result18.class);

            //Print and Save Results
            result.map(p-> Result18.toTuple(p)).returns(new TypeHint<Tuple6<Float,String,Float,Integer,Integer,String>>(){})
                    .writeAsCsv(outputAddress + TAG + "-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG, Util.STATUS_DONE);

        } catch (Exception e) {
            e.printStackTrace();
            Util.jobLog(TAG, Util.STATUS_FAIELD);
        }
        env.execute("18-QUERY");
    }
}
