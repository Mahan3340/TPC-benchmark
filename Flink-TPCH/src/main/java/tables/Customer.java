package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Customer {

    public Integer C_CUSTKEY;
    public String C_NAME;
    public String C_ADDRESS;
    public Integer C_NATIONKEY;
    public String C_PHONE;
    public Float C_ACCTBAL;
    public String C_MKTSEGMENT;
    public String C_COMMENT;

    public Customer() {
    }

    public Integer getC_CUSTKEY() {
        return C_CUSTKEY;
    }

    public void setC_CUSTKEY(Integer c_CUSTKEY) {
        C_CUSTKEY = c_CUSTKEY;
    }

    public String getC_NAME() {
        return C_NAME;
    }

    public void setC_NAME(String c_NAME) {
        C_NAME = c_NAME;
    }

    public String getC_ADDRESS() {
        return C_ADDRESS;
    }

    public void setC_ADDRESS(String c_ADDRESS) {
        C_ADDRESS = c_ADDRESS;
    }

    public Integer getC_NATIONKEY() {
        return C_NATIONKEY;
    }

    public void setC_NATIONKEY(Integer c_NATIONKEY) {
        C_NATIONKEY = c_NATIONKEY;
    }

    public String getC_PHONE() {
        return C_PHONE;
    }

    public void setC_PHONE(String c_PHONE) {
        C_PHONE = c_PHONE;
    }

    public Float getC_ACCTBAL() {
        return C_ACCTBAL;
    }

    public void setC_ACCTBAL(Float c_ACCTBAL) {
        C_ACCTBAL = c_ACCTBAL;
    }

    public String getC_MKTSEGMENT() {
        return C_MKTSEGMENT;
    }

    public void setC_MKTSEGMENT(String c_MKTSEGMENT) {
        C_MKTSEGMENT = c_MKTSEGMENT;
    }

    public String getC_COMMENT() {
        return C_COMMENT;
    }

    public void setC_COMMENT(String c_COMMENT) {
        C_COMMENT = c_COMMENT;
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path(address + "customer.avro");
        AvroInputFormat<Customer> format = new AvroInputFormat<Customer>(path, Customer.class);
        final DataSet<Customer> customerDataSet = env.createInput(format);
        Table customer = tEnv.fromDataSet(customerDataSet);
        return customer;
    }

}
