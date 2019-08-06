package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Part {

    public Integer P_PARTKEY;
    public String P_NAME;
    public String P_MFGR;
    public String P_BRAND;
    public String P_TYPE;
    public Integer P_SIZE;
    public String P_CONTAINER;
    public Float P_RETAILPRICE;
    public String P_COMMENT;

    public Part() {
    }

    public Integer getP_PARTKEY() {
        return P_PARTKEY;
    }

    public void setP_PARTKEY(Integer p_PARTKEY) {
        P_PARTKEY = p_PARTKEY;
    }

    public String getP_NAME() {
        return P_NAME;
    }

    public void setP_NAME(String p_NAME) {
        P_NAME = p_NAME;
    }

    public String getP_MFGR() {
        return P_MFGR;
    }

    public void setP_MFGR(String p_MFGR) {
        P_MFGR = p_MFGR;
    }

    public String getP_BRAND() {
        return P_BRAND;
    }

    public void setP_BRAND(String p_BRAND) {
        P_BRAND = p_BRAND;
    }

    public String getP_TYPE() {
        return P_TYPE;
    }

    public void setP_TYPE(String p_TYPE) {
        P_TYPE = p_TYPE;
    }

    public Integer getP_SIZE() {
        return P_SIZE;
    }

    public void setP_SIZE(Integer p_SIZE) {
        P_SIZE = p_SIZE;
    }

    public String getP_CONTAINER() {
        return P_CONTAINER;
    }

    public void setP_CONTAINER(String p_CONTAINER) {
        P_CONTAINER = p_CONTAINER;
    }

    public Float getP_RETAILPRICE() {
        return P_RETAILPRICE;
    }

    public void setP_RETAILPRICE(Float p_RETAILPRICE) {
        P_RETAILPRICE = p_RETAILPRICE;
    }

    public String getP_COMMENT() {
        return P_COMMENT;
    }

    public void setP_COMMENT(String p_COMMENT) {
        P_COMMENT = p_COMMENT;
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path(address + "part.avro");
        AvroInputFormat<Part> format = new AvroInputFormat<Part>(path, Part.class);
        final DataSet<Part> partDataSet = env.createInput(format);
        Table part = tEnv.fromDataSet(partDataSet);
        return part;
    }
}
