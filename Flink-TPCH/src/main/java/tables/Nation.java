package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Nation {

    public Integer N_NATIONKEY;
    public String N_NAME;
    public Integer N_REGIONKEY;
    public String N_COMMENT;


    public Integer getN_NATIONKEY() {
        return N_NATIONKEY;
    }

    public void setN_NATIONKEY(Integer n_NATIONKEY) {
        N_NATIONKEY = n_NATIONKEY;
    }

    public String getN_NAME() {
        return N_NAME;
    }

    public void setN_NAME(String n_NAME) {
        N_NAME = n_NAME;
    }

    public Integer getN_REGIONKEY() {
        return N_REGIONKEY;
    }

    public void setN_REGIONKEY(Integer n_REGIONKEY) {
        N_REGIONKEY = n_REGIONKEY;
    }

    public String getN_COMMENT() {
        return N_COMMENT;
    }

    public void setN_COMMENT(String n_COMMENT) {
        N_COMMENT = n_COMMENT;
    }
    public Nation() {
    }

    public static Tuple4<Integer, String, Integer, String> toTuple(Nation nation) {
        return Tuple4.of(nation.N_NATIONKEY,nation.N_NAME,nation.N_REGIONKEY,nation.N_COMMENT);
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path( address + "nation.avro");
        AvroInputFormat<Nation> format = new AvroInputFormat<Nation>(path, Nation.class);
        final DataSet<Nation> nationDataSet = env.createInput(format);
        Table nation = tEnv.fromDataSet(nationDataSet);
        return nation;
    }
}
