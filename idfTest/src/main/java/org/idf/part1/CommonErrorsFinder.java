package org.idf.part1;
import java.util.*;
import java.util.concurrent.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommonErrorsFinder {
    private static final String ERROR_PATTERN = "ERR_\\d+";
    private List<String> lines=new ArrayList<>();
    private Map<String, Integer> errorTypesCounters; //סיבוכיות מקום O(n) הולך בחזקות
    //config :
    private String logFilePath;
    private int topErrsAmountRequired;
    private int numberOfChunksRequired;
    private boolean isFileChanged=false;
    private boolean isFileLoadedSuccessfully;
    public CommonErrorsFinder(String logFilePath, int n){
        this.logFilePath=logFilePath;
        this.isFileLoadedSuccessfully=loadFileData();
        this.topErrsAmountRequired=n;
        this.errorTypesCounters=new HashMap<>();
        this.numberOfChunksRequired = Runtime.getRuntime().availableProcessors();
        System.out.println("number of cores"+this.numberOfChunksRequired);
    }
    public CommonErrorsFinder(String logFilePath, int n, boolean isFileChanged){
        this(logFilePath,n);
        this.isFileChanged=isFileChanged;
    }
    protected boolean loadFileData(){
        boolean isLoadingSucceeded=false;
        File file = new File(logFilePath);
        if (file.exists() && file.isFile()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) { //סיבוכיות זמן ריצה O(n)
                    lines.add(line);//loading into list since processing on each line only seperately..
                }
                isLoadingSucceeded=true;
            } catch (IOException e) {
                e.printStackTrace();
                isLoadingSucceeded=false;
            }
        } else {
            System.err.println("File not found: " + logFilePath);
            isLoadingSucceeded=false;
        }
        return isLoadingSucceeded;
    }
    public Map<String, Integer> getTopCommonErrorsTypes() throws ExecutionException, InterruptedException {
        Map<String, Integer> picedTopValues=null;
        if(!isFileLoadedSuccessfully){
            System.out.println("loading failed");
        }else{
            if(errorTypesCounters.size()==0 || isFileChanged){
                findTopCommonErrorsTypes();
            }
            picedTopValues=picTopValues();
        }
        return picedTopValues;
    }
    private void findTopCommonErrorsTypes() throws InterruptedException, ExecutionException {
        List<Callable<Map<String, Integer>>> tasks = new ArrayList<>();
        ExecutorService executorService= Executors.newFixedThreadPool(this.numberOfChunksRequired);

        int numberOfLines = lines.size();
        int chunkSize=numberOfLines/this.numberOfChunksRequired;

        for(int i=0;i<numberOfLines;i+=chunkSize){//סיבוכיות ריצה O(n)
           int startIndex=i;
            tasks.add(()->updateErrCounter(this.lines.subList(startIndex,startIndex+chunkSize)));// sub list worst case O(1)
        }
        List<Future<Map<String, Integer>>> results = executorService.invokeAll(tasks);

        for (Future<Map<String, Integer>> future : results) {
            Map<String,Integer> map= future.get();
            for(Map.Entry<String,Integer> entry:map.entrySet()){
                this.errorTypesCounters.merge(entry.getKey(),entry.getValue(),Integer::sum);// merge worst case O(N)
            }
        }
        executorService.shutdown();
    }
    private Map<String, Integer> updateErrCounter(List<String> lines) {
        Map<String, Integer> chunkErrorTypesCounters=new HashMap<>();
        Pattern pattern = Pattern.compile(ERROR_PATTERN);

        lines.stream().forEach((line)->{//סיבוכיות ריצה של O(n)
            Matcher matcher=pattern.matcher(line);
            String errorType="";
            if(matcher.find()){
                errorType=matcher.group(0);
            }
            Integer prevCnt=chunkErrorTypesCounters.get(errorType);  //O(1) אין כפילויותולכן הסיבוכיות טובה
            if(errorType!=""){
                if(prevCnt==null && errorType!=""){
                    chunkErrorTypesCounters.put(errorType,1);
                }else{
                    chunkErrorTypesCounters.put(errorType,prevCnt+1);
                }
            }
        });

        return chunkErrorTypesCounters;
    }
    protected Map<String, Integer> picTopValues(){

        System.out.println(errorTypesCounters);
        return errorTypesCounters.entrySet().stream()
                .sorted(Map.Entry.comparingByValue((c1, c2) -> c2.compareTo(c1)))
                .limit(this.topErrsAmountRequired) //סיבוכיות ריצה של O(K)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

//    סיבוכיות הפיתרון הוא O(n log n) בגלל הsorted
}
