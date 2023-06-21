import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import okhttp3.*;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

public class TransactionSimulator {
    private static String API_ENDPOINT = "http://172.28.48.201:8092/cfrt-adapter-ws/getFraudScore";
    private static int THREAD_POOL_SIZE = 5;  //Number of concurrent users

    private static final  OkHttpClient client = new OkHttpClient();

    private static final ObjectMapper objectMapper = new ObjectMapper();




    private static String FILE_PATH="transactions.csv";

    public static void main(String[] args) {
        // Read CSV file
//        if (args[0]!=null || args[0]!=""){
//            API_ENDPOINT=args[0].trim();
//        }
//        if (args[1] != null || args[1]!="") {
//            THREAD_POOL_SIZE=parseInt(args[1].trim());
//
//
//        }
        API_ENDPOINT = (args.length > 0 && args[0] != null && !args[0].isEmpty()) ? args[0].trim() : API_ENDPOINT;
        THREAD_POOL_SIZE = (args.length > 1 && args[1] != null && !args[1].isEmpty()) ? Integer.parseInt(args[1].trim()) : THREAD_POOL_SIZE;
        FILE_PATH = (args.length > 2 && args[2] != null && !args[2].isEmpty()) ? args[2].trim() : FILE_PATH;




        List<String[]> csvRecords = readCSVFile(FILE_PATH);
        int len;
        try {
            len=csvRecords.size();
        }    catch (NullPointerException e){
            len=0;
        }
        System.out.println("Number of records: " + len);

         //Create a thread pool with a fixed number of threads
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        long startTime=System.currentTimeMillis();
        //System.out.printf(System.cu);
        // Submit tasks to the thread pool

//        for (String[] record : csvRecords) {
//            Transaction transaction = createTransaction(record);
//            Runnable task = createTask(transaction);
//            executorService.submit(task);
//        }


        try(CSVReader csvReader= new CSVReader(new FileReader(FILE_PATH))){
            String [] nextLine;
            while((nextLine=csvReader.readNext())!=null){
                Transaction transaction=createTransaction(nextLine);
                Runnable task=createTask(transaction);
                executorService.submit(task);
            }
        }catch (IOException | CsvException e){
            System.out.println("ERR");
        }

        // Shutdown the thread pool after all tasks are completed

        executorService.shutdown();

        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            System.out.println("Length of the file = "+len);
            long processedTime = System.currentTimeMillis();
            long proccesTime = processedTime - startTime;
            double tps = (double) len / (proccesTime / 1000.0);
            System.out.println("TPS: " + tps);
        }catch (InterruptedException e){
            System.out.println(e.getMessage());
        }
}

    private static List<String[]> readCSVFile(String csvFilePath) {
        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
//            csvReader.re
            return csvReader.readAll();
        } catch (IOException | CsvException e) {
            e.printStackTrace();
            return null;
        }
    }


//    private static void readFileAndSendRequest(String csvFilePath){
//
//    }
    private static String[] readLinesCSVFile(String csvFilePath){
        try(CSVReader csvReader=new CSVReader(new FileReader(csvFilePath))){
            return csvReader.readNext();
        }catch(IOException  | CsvException e){
            e.printStackTrace();
            return null;
        }
    }

    private static Transaction createTransaction(String[] record) {


        //String country record[0];
        String counterpartName= record[0];
        String channel =record[1];
        String txnType =record[2];
        String beneficiaryAccountID= record[3];
        String txnDateTime =record[4];
        String responseCode =record[5];
        String accountID =record[6];
        String counterpartBank= record[7];
        String txnDesc =record[8];
        String acEntrySRNo= record[9];
        String txnMode =record[10];
        String node =record[11];
        String txnBranch= record[12];
        String txnAmount =record[13];
        String mcc =record[14];


        return new Transaction( counterpartName,  channel,  txnType,  beneficiaryAccountID,  txnDateTime,  responseCode,  accountID,  counterpartBank,  txnDesc,  acEntrySRNo,  txnMode,  node,  txnBranch,  txnAmount,  mcc);

    }

    private static Runnable createTask(Transaction transaction) {
        return () -> {
            sendPostRequest(transaction);
        };
    }

    private static void sendPostRequest(Transaction transaction) {


        String jsonPayload;
        try {
            jsonPayload = objectMapper.writeValueAsString(transaction);
            //System.out.printf(jsonPayload);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        MediaType mediaType=MediaType.parse("application/json");

        RequestBody body = RequestBody.create(mediaType,jsonPayload);
        Request request = new Request.Builder()
                .url(API_ENDPOINT)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            System.out.println("Request sent successfully: " + response.code());
            System.out.println("Resp::"+response.body().string());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Transaction implements Serializable {
        //private String country;
        private String counterpartName;
        private String channel;
        private String txnType;
        private String beneficiaryAccountID;
        private String txnDateTime;
        private String responseCode;
        private String accountID;
        private String counterpartBank;
        private String txnDesc;
        private String acEntrySRNo;
        private String txnMode;
        private String node;
        private String txnBranch;
        private String txnAmount;
        private String mcc;

        public String getCounterpartName() {
            return counterpartName;
        }

        public void setCounterpartName(String counterpartName) {
            this.counterpartName = counterpartName;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }

        public String getTxnType() {
            return txnType;
        }

        public void setTxnType(String txnType) {
            this.txnType = txnType;
        }

        public String getBeneficiaryAccountID() {
            return beneficiaryAccountID;
        }

        public void setBeneficiaryAccountID(String beneficiaryAccountID) {
            this.beneficiaryAccountID = beneficiaryAccountID;
        }

        public String getTxnDateTime() {
            return txnDateTime;
        }

        public void setTxnDateTime(String txnDateTime) {
            this.txnDateTime = txnDateTime;
        }

        public String getResponseCode() {
            return responseCode;
        }

        public void setResponseCode(String responseCode) {
            this.responseCode = responseCode;
        }

        public String getAccountID() {
            return accountID;
        }

        public void setAccountID(String accountID) {
            this.accountID = accountID;
        }

        public String getCounterpartBank() {
            return counterpartBank;
        }

        public void setCounterpartBank(String counterpartBank) {
            this.counterpartBank = counterpartBank;
        }

        public String getTxnDesc() {
            return txnDesc;
        }

        public void setTxnDesc(String txnDesc) {
            this.txnDesc = txnDesc;
        }

        public String getAcEntrySRNo() {
            return acEntrySRNo;
        }

        public void setAcEntrySRNo(String acEntrySRNo) {
            this.acEntrySRNo = acEntrySRNo;
        }

        public String getTxnMode() {
            return txnMode;
        }

        public void setTxnMode(String txnMode) {
            this.txnMode = txnMode;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public String getTxnBranch() {
            return txnBranch;
        }

        public void setTxnBranch(String txnBranch) {
            this.txnBranch = txnBranch;
        }

        public String getTxnAmount() {
            return txnAmount;
        }

        public void setTxnAmount(String txnAmount) {
            this.txnAmount = txnAmount;
        }

        public String getMcc() {
            return mcc;
        }

        public void setMcc(String mcc) {
            this.mcc = mcc;
        }

        public Transaction(String counterpartName, String channel, String txnType, String beneficiaryAccountID, String txnDateTime, String responseCode, String accountID, String counterpartBank, String txnDesc, String acEntrySRNo, String txnMode, String node, String txnBranch, String txnAmount, String mcc) {
            //this.country = country;
            this.counterpartName = counterpartName;
            this.channel = channel;
            this.txnType = txnType;
            this.beneficiaryAccountID = beneficiaryAccountID;
            this.txnDateTime = txnDateTime;
            this.responseCode = responseCode;
            this.accountID = accountID;
            this.counterpartBank = counterpartBank;
            this.txnDesc = txnDesc;
            this.acEntrySRNo = acEntrySRNo;
            this.txnMode = txnMode;
            this.node = node;
            this.txnBranch = txnBranch;
            this.txnAmount = txnAmount;
            this.mcc = mcc;
        }
        // Getters and setters (or use lombok for automatic generation)
    }
}

//{
//        "country":"${COUNTRY}",
//        "counterpartName":"${COUNTERPARTNAME}",
//        "channel":"${CHANNEL}",
//        "txnType":"${TXNTYPE}",
//        "beneficiaryAccountID":"${BENEFICIARYACCOUNTID}",
//        "txnDateTime":"${TXNDATETIME}",
//        "responseCode":"${RESPONSECODE}",
//        "accountID":"${ACCOUNTID}",
//        "counterpartBank":"${COUNTERPART_BANK}",
//        "txnDesc":"${TXNDESC}",
//        "acEntrySRNo":"${ACENTRYSRNO}",
//        "txnMode":"${TXNMODE}",
//        "node":"${NODE}",
//        "txnBranch":"${TXNBRANCH}",
//        "txnAmount":"${TXNAMOUNT}",
//        "mcc":"${MCC}"
//        }
