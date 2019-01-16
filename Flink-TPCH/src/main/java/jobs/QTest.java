package jobs;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import results.TestResult;
import tables.*;
import utils.Util;
import java.util.HashMap;

public class QTest {

    public final static String TAG = "QTEST";
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

   //         Util.jobLog(TAG,Util.STATUS_START);

            Path nationTestPath = new Path(dataAddress + "nation.avro");
            AvroInputFormat<Nation> nationTestAvroFormat = new AvroInputFormat<>(nationTestPath,Nation.class);

            //Create Tables
            final DataSet<Nation> nationTestDataSet = env.createInput(nationTestAvroFormat);
            Table nationTest = tEnv.fromDataSet(nationTestDataSet);


            //Query
            Table result = nationTest
                    .filter("N_NATIONKEY > 2")
                    .select("N_NAME as NAME_TEST");

            DataSet<TestResult> resultSet = tEnv.toDataSet(result,TestResult.class);
            resultSet.map(p -> TestResult.toTuple(p)).returns(new TypeHint<Tuple1<String>>() {})
                    .writeAsCsv(outputAddress + TAG +"-res.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

            Util.jobLog(TAG,Util.STATUS_DONE);

        }catch (Exception e)
        {
            Util.jobLog(TAG,Util.STATUS_FAIELD);
            e.printStackTrace();
        }

        env.execute("Test-Query");
    }
}
