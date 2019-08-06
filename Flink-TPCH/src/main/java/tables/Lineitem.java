package tables;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import javax.sound.sampled.Line;
import java.io.Serializable;

public class Lineitem {

    public Integer L_ORDERKEY; //0
    public Integer L_PARTKEY; //1
    public Integer L_SUPPKEY; //2
    public Integer L_LINENUMBER; //3
    public Float L_QUANTITY; //4
    public Float L_EXTENDEDPRICE; //5
    public Float L_DISCOUNT; //6
    public Float L_TAX; //7
    public String L_RETURNFLAG; //8
    public String L_LINESTATUS; //9
    public String L_SHIPDATE; //10
    public String L_COMMITDATE; //11
    public String L_RECEIPTDATE; //12
    public String L_SHIPINSTRUCT; //13
    public String L_SHIPMODE; //14
    public String L_COMMENT; //15

    private static final long serialVersionUID = 6529685098267757692L;


    public Integer getL_ORDERKEY() {
        return L_ORDERKEY;
    }

    public void setL_ORDERKEY(Integer l_ORDERKEY) {
        L_ORDERKEY = l_ORDERKEY;
    }

    public Integer getL_PARTKEY() {
        return L_PARTKEY;
    }

    public void setL_PARTKEY(Integer l_PARTKEY) {
        L_PARTKEY = l_PARTKEY;
    }

    public Integer getL_SUPPKEY() {
        return L_SUPPKEY;
    }

    public void setL_SUPPKEY(Integer l_SUPPKEY) {
        L_SUPPKEY = l_SUPPKEY;
    }

    public Integer getL_LINENUMBER() {
        return L_LINENUMBER;
    }

    public void setL_LINENUMBER(Integer l_LINENUMBER) {
        L_LINENUMBER = l_LINENUMBER;
    }

    public Float getL_QUANTITY() {
        return L_QUANTITY;
    }

    public void setL_QUANTITY(Float l_QUANTITY) {
        L_QUANTITY = l_QUANTITY;
    }

    public Float getL_EXTENDEDPRICE() {
        return L_EXTENDEDPRICE;
    }

    public void setL_EXTENDEDPRICE(Float l_EXTENDEDPRICE) {
        L_EXTENDEDPRICE = l_EXTENDEDPRICE;
    }

    public Float getL_DISCOUNT() {
        return L_DISCOUNT;
    }

    public void setL_DISCOUNT(Float l_DISCOUNT) {
        L_DISCOUNT = l_DISCOUNT;
    }

    public Float getL_TAX() {
        return L_TAX;
    }

    public void setL_TAX(Float l_TAX) {
        L_TAX = l_TAX;
    }

    public String getL_RETURNFLAG() {
        return L_RETURNFLAG;
    }

    public void setL_RETURNFLAG(String l_RETURNFLAG) {
        L_RETURNFLAG = l_RETURNFLAG;
    }

    public String getL_LINESTATUS() {
        return L_LINESTATUS;
    }

    public void setL_LINESTATUS(String l_LINESTATUS) {
        L_LINESTATUS = l_LINESTATUS;
    }

    public String getL_SHIPDATE() {
        return L_SHIPDATE;
    }

    public void setL_SHIPDATE(String l_SHIPDATE) {
        L_SHIPDATE = l_SHIPDATE;
    }

    public String getL_COMMITDATE() {
        return L_COMMITDATE;
    }

    public void setL_COMMITDATE(String l_COMMITDATE) {
        L_COMMITDATE = l_COMMITDATE;
    }

    public String getL_RECEIPTDATE() {
        return L_RECEIPTDATE;
    }

    public void setL_RECEIPTDATE(String l_RECEIPTDATE) {
        L_RECEIPTDATE = l_RECEIPTDATE;
    }

    public String getL_SHIPINSTRUCT() {
        return L_SHIPINSTRUCT;
    }

    public void setL_SHIPINSTRUCT(String l_SHIPINSTRUCT) {
        L_SHIPINSTRUCT = l_SHIPINSTRUCT;
    }

    public String getL_SHIPMODE() {
        return L_SHIPMODE;
    }

    public void setL_SHIPMODE(String l_SHIPMODE) {
        L_SHIPMODE = l_SHIPMODE;
    }

    public String getL_COMMENT() {
        return L_COMMENT;
    }

    public void setL_COMMENT(String l_COMMENT) {
        L_COMMENT = l_COMMENT;
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env,org.apache.flink.table.api.java.BatchTableEnvironment tEnv,String address)
    {
        Path path = new Path(address + "lineitem.avro");
        AvroInputFormat<Lineitem> format = new AvroInputFormat<Lineitem>(path, Lineitem.class);
        final DataSet<Lineitem> customerDataSet = env.createInput(format);
        Table lineitem = tEnv.fromDataSet(customerDataSet);
        return lineitem;
    }
}
