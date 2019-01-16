package results;

import org.apache.flink.api.java.tuple.Tuple9;

import java.io.Serializable;

public class Result1 {

    public String L_RETURNFLAG;
    public String L_LINESTATUS;
    public Float SUM_QTY;
    public Float SUM_DISC_PRICE;
    public Float SUM_BASE_PRICE;
    public Float SUM_CHARGE;
    public Float AVG_QTY;
    public Float AVG_PRICE;
    public Float AVG_DISC;


    public Result1() {

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

    public float getSUM_QTY() {
        return SUM_QTY;
    }

    public void setSUM_QTY(float SUM_QTY) {
        this.SUM_QTY = SUM_QTY;
    }

    public Float getSUM_DISC_PRICE() {
        return SUM_DISC_PRICE;
    }

    public void setSUM_DISC_PRICE(Float SUM_DISC_PRICE) {
        this.SUM_DISC_PRICE = SUM_DISC_PRICE;
    }

    public Float getSUM_BASE_PRICE() {
        return SUM_BASE_PRICE;
    }

    public void setSUM_BASE_PRICE(Float SUM_BASE_PRICE) {
        this.SUM_BASE_PRICE = SUM_BASE_PRICE;
    }

    public Float getSUM_CHARGE() {
        return SUM_CHARGE;
    }

    public void setSUM_CHARGE(Float SUM_CHARGE) {
        this.SUM_CHARGE = SUM_CHARGE;
    }

    public Float getAVG_QTY() {
        return AVG_QTY;
    }

    public void setAVG_QTY(Float AVG_QTY) {
        this.AVG_QTY = AVG_QTY;
    }

    public Float getAVG_PRICE() {
        return AVG_PRICE;
    }

    public void setAVG_PRICE(Float AVG_PRICE) {
        this.AVG_PRICE = AVG_PRICE;
    }

    public Float getAVG_DISC() {
        return AVG_DISC;
    }

    public void setAVG_DISC(Float AVG_DISC) {
        this.AVG_DISC = AVG_DISC;
    }

    public static Tuple9<String,String,Float,Float,Float,Float,Float,Float,Float> toTuple(Result1 result1) {
        return Tuple9.of(result1.getL_RETURNFLAG(),result1.getL_LINESTATUS(),result1.getSUM_QTY(),result1.getSUM_DISC_PRICE(),
                result1.getSUM_BASE_PRICE(),result1.getSUM_CHARGE(),result1.getAVG_QTY(),result1.getAVG_PRICE(),result1.getAVG_DISC());
    }


}
