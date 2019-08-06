package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Supplier {
    public Integer S_SUPPKEY;
    public String S_NAME;
    public String S_ADDRESS;
    public Integer S_NATIONKEY;
    public String S_PHONE;
    public Float S_ACCTBAL;
    public String S_COMMENT;


    public Supplier() {
    }

    public Integer getS_SUPPKEY() {
        return S_SUPPKEY;
    }

    public void setS_SUPPKEY(Integer s_SUPPKEY) {
        S_SUPPKEY = s_SUPPKEY;
    }

    public String getS_NAME() {
        return S_NAME;
    }

    public void setS_NAME(String s_NAME) {
        S_NAME = s_NAME;
    }

    public String getS_ADDRESS() {
        return S_ADDRESS;
    }

    public void setS_ADDRESS(String s_ADDRESS) {
        S_ADDRESS = s_ADDRESS;
    }

    public Integer getS_NATIONKEY() {
        return S_NATIONKEY;
    }

    public void setS_NATIONKEY(Integer s_NATIONKEY) {
        S_NATIONKEY = s_NATIONKEY;
    }

    public String getS_PHONE() {
        return S_PHONE;
    }

    public void setS_PHONE(String s_PHONE) {
        S_PHONE = s_PHONE;
    }

    public Float getS_ACCTBAL() {
        return S_ACCTBAL;
    }

    public void setS_ACCTBAL(Float s_ACCTBAL) {
        S_ACCTBAL = s_ACCTBAL;
    }

    public String getS_COMMENT() {
        return S_COMMENT;
    }

    public void setS_COMMENT(String s_COMMENT) {
        S_COMMENT = s_COMMENT;
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path(address + "supplier.avro");
        AvroInputFormat<Supplier> format = new AvroInputFormat<Supplier>(path, Supplier.class);
        final DataSet<Supplier> supplierDataSet = env.createInput(format);
        Table supplier = tEnv.fromDataSet(supplierDataSet);
        return supplier;
    }
}
