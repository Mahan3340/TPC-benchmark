package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class PartSupp {

    public Integer PS_PARTKEY;
    public Integer PS_SUPPKEY;
    public Integer PS_AVAILQTY;
    public Float PS_SUPPLYCOST;
    public String PS_COMMENT;

    public PartSupp() {
    }

    public Integer getPS_PARTKEY() {
        return PS_PARTKEY;
    }

    public void setPS_PARTKEY(Integer PS_PARTKEY) {
        this.PS_PARTKEY = PS_PARTKEY;
    }

    public Integer getPS_SUPPKEY() {
        return PS_SUPPKEY;
    }

    public void setPS_SUPPKEY(Integer PS_SUPPKEY) {
        this.PS_SUPPKEY = PS_SUPPKEY;
    }

    public Integer getPS_AVAILQTY() {
        return PS_AVAILQTY;
    }

    public void setPS_AVAILQTY(Integer PS_AVAILQTY) {
        this.PS_AVAILQTY = PS_AVAILQTY;
    }

    public Float getPS_SUPPLYCOST() {
        return PS_SUPPLYCOST;
    }

    public void setPS_SUPPLYCOST(Float PS_SUPPLYCOST) {
        this.PS_SUPPLYCOST = PS_SUPPLYCOST;
    }

    public String getPS_COMMENT() {
        return PS_COMMENT;
    }

    public void setPS_COMMENT(String PS_COMMENT) {
        this.PS_COMMENT = PS_COMMENT;
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path(address + "partsupp.avro");
        AvroInputFormat<PartSupp> format = new AvroInputFormat<PartSupp>(path, PartSupp.class);
        final DataSet<PartSupp> partSuppDataSet = env.createInput(format);
        Table partsupp = tEnv.fromDataSet(partSuppDataSet);
        return partsupp;
    }
}
