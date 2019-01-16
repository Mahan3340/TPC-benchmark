//
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.configuration.Configuration;
//
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//
//public  class NationMapper extends RichMapFunction<String, tables.Nation> {
//    DateFormat fs = new SimpleDateFormat("yyyy-MM-dd");
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//
//    }
//
////    @Override
////    public tables.Nation map(String s) throws Exception {
////        getRuntimeContext().getLongCounter("elements").add(1L);
////
////        String[] nation = s.split("\\|");
////
////        //return new tables.Nation(Integer.parseInt(nation[0]),nation[1],Integer.parseInt(nation[2]),nation[3]);
////    }
//}