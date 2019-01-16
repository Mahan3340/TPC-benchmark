package utils;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Util {

    public final static int STATUS_FAIELD = 0;
    public final static int STATUS_START = 1;
    public final static int STATUS_DONE = 2;

    public static PrintStream fileOut;
    public static PrintStream fileErr;
    public static PrintStream orgOut;
    public static PrintStream orgErr;

    public static void setPrintStream(PrintStream orgOut, PrintStream orgErr, PrintStream fileOut, PrintStream fileErr) {
        Util.fileOut = fileOut;
        Util.fileErr = fileErr;
        Util.orgErr = orgErr;
        Util.orgOut = orgOut;

        System.setOut(fileOut);
        System.setErr(fileErr);
    }

    public static void log(String message, String tag) {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        Date dt = new Date();
        String date = sdf.format(dt);
        orgOut.println(date + " - " + tag + " - " +  message);
        fileOut.println(date + " - " + tag + " - " + message);
    }

    public static void jobLog(String tag, int status) {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        Date dt = new Date();
        String date = sdf.format(dt);
        ;
        switch (status) {
            case STATUS_DONE:
                orgOut.println((date + " - " + tag + " - " + "STATUS = Running Query and Writing Results"));
                fileOut.println((date + " - " + tag + " - " + "STATUS = Running Query and Writing Results"));
                break;
            case STATUS_FAIELD:
                orgOut.println(date + " - " + tag + " - " + "STATUS = FAIELD - Check Exception Logs for the Reason");
                fileOut.println(date + " - " + tag + " - " + "STATUS = FAIELD - Check Exception Logs for the Reason");
                break;
            case STATUS_START:
                orgOut.println((date + " - " + tag + " - " + "STATUS = STARTED"));
                fileOut.println((date + " - " + tag + " - " + "STATUS = STARTED"));
                break;
        }
    }

    public static void jobLog(String message, String tag, int status) {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        Date dt = new Date();
        String date = sdf.format(dt);
        ;
        switch (status) {

            case STATUS_DONE:
                orgOut.println((date + " - " + message + " - " + tag + " - " + "STATUS = Running Query and Writing Results"));
                fileOut.println((date + " - " + message + " - " + tag + " - " + "STATUS = Running Query and Writing Results"));
                break;
            case STATUS_FAIELD:
                orgOut.println(date + " - " + message + " - " + tag + " - " + "STATUS = FAIELD - Check Exception Logs for the Reason");
                fileOut.println(date + " - " + message + " - " + tag + " - " + "STATUS = FAIELD - Check Exception Logs for the Reason");
                break;
            case STATUS_START:
                orgOut.println((date + " - " + date + " - " + message + " - " + tag + " - " + "STATUS = STARTED"));
                fileOut.println((date + " - " + date + " - " + message + " - " + tag + " - " + "STATUS = STARTED"));
                break;
        }
    }

    public static HashMap<String,String> readAddress(String TAG) {
        HashMap<String, String> result = new HashMap<>();
        try {

            Scanner scanner = new Scanner(System.in);

            System.out.println("");
            System.out.println("");

            System.out.println("Enter Log-Output Address:");
            System.out.println("Note1: This should not be a HDFS address and the folders should already been created, for example /my-results/ ");
            System.out.println("Note2: I won't check for address validity be Nice and enter a Valid one :) ");
            System.out.println("Note3: Don't forget the / at both sides ");
            System.out.println("Address:");
            String logAddress = scanner.nextLine();

            System.out.println("Enter Query-Output Address:");
            System.out.println("Note1: This address should be a HDFS address and The folders should already been created, for example  hdfs://namenode:8020/results/");
            System.out.println("Note2: Consider Note2 Above Again");
            System.out.println("Note3: The file that the output is going to be saved on will be overwritten, be careful");
            System.out.println("Note4: Don't forget the / at both sides ");
            System.out.println("Address:");
            String outputAddress = scanner.nextLine();

            System.out.println("Enter Data Root Address");
            System.out.println("Note1: This address should be a HDFS address and the folder that contains all tables in avro format, for example hdfs://namenode:8020/my-data/ ");
            System.out.println("Note2: Don't forget the / at both sides ");
            System.out.println("Address:");
            String dataAddress = scanner.nextLine();


           // Save Logs to file
            PrintStream orgOut = System.out;
            PrintStream orgError = System.out;
            PrintStream fileOut = new PrintStream(logAddress + TAG + "-out.txt");
            PrintStream errorOut = new PrintStream(logAddress + TAG + "-error.txt");
            Util.setPrintStream(orgOut, orgError, fileOut, errorOut);

            Util.log("Print Logs will be saved on " + logAddress + TAG +"-out.txt", TAG);
            Util.log("Exception Logs will be saved on " + logAddress + TAG + "-error.txt", TAG);
            Util.log("Query Output will be saved on " + outputAddress + TAG + "-res.csv", TAG);
            result.put("log",logAddress);
            result.put("out",outputAddress);
            result.put("data",dataAddress);

            return result;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;

    }
}
