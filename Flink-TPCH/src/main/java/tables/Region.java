package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Region {

    public Integer R_REGIONKEY;
    public String R_NAME;
    public String R_COMMENT;

    public Region() {
    }

    public Integer getR_REGIONKEY() {
        return R_REGIONKEY;
    }

    public void setR_REGIONKEY(Integer r_REGIONKEY) {
        R_REGIONKEY = r_REGIONKEY;
    }

    public String getR_NAME() {
        return R_NAME;
    }

    public void setR_NAME(String r_NAME) {
        R_NAME = r_NAME;
    }

    public String getR_COMMENT() {
        return R_COMMENT;
    }

    public void setR_COMMENT(String r_COMMENT) {
        R_COMMENT = r_COMMENT;
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path(address + "region.avro");
        AvroInputFormat<Region> format = new AvroInputFormat<Region>(path, Region.class);
        final DataSet<Region> regionDataSet = env.createInput(format);
        Table region = tEnv.fromDataSet(regionDataSet);
        return region;
    }
}