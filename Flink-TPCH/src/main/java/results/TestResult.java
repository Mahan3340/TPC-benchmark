package results;

import org.apache.flink.api.java.tuple.Tuple1;

public class TestResult {


    public String NAME_TEST;

    public TestResult() {
    }

    public String getNAME_TEST() {
        return NAME_TEST;
    }

    public void setNAME_TEST(String NAME_TEST) {
        this.NAME_TEST = NAME_TEST;
    }

    public static Tuple1<String> toTuple(TestResult result23) {
        return Tuple1.of(result23.getNAME_TEST());
    }
}
