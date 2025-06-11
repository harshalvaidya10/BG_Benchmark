/**
 * @description BGCoord for janusgraph
 * @date 2025/3/27 11:02
 * @version 1.0
 */

package edu.usc.bg;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static edu.usc.bg.SSHExecutor.*;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphBGCoord {

    private String workload;
    private String populateWorkload;
    private double latency;
    private double perc;
    private double staleness;
    private int duration;
    private String janusGraphIp;
    private String directory;
    private int minimum;
    private String objective;
    private boolean validation;
    private boolean doLoad = false;
    private boolean doCache = true;
    private boolean doMonitor = false;
    private boolean doWarmup = true;
    private boolean isWrite = false;
    Properties props = new Properties();
    Properties coreProps = new Properties();

    private static final class Stat {
        final double tp;
        final boolean sla;
        Stat(double tp, boolean sla) { this.tp = tp; this.sla = sla; }
    }

    public boolean ifWriteWorkload() {
        float acceptFriendReqAction = Float.parseFloat(coreProps.getProperty("AcceptFriendReqAction"));
        float rejectFriendReqAction = Float.parseFloat(coreProps.getProperty("RejectFriendReqAction"));
        float thawFriendshipAction = Float.parseFloat(coreProps.getProperty("ThawFriendshipAction"));
        float inviteFriendAction = Float.parseFloat(coreProps.getProperty("InviteFriendAction"));
        return acceptFriendReqAction > 0 || rejectFriendReqAction > 0 || thawFriendshipAction > 0 || inviteFriendAction > 0;
    }

    public static void main(String[] args) throws Exception {
        JanusGraphBGCoord coord = new JanusGraphBGCoord();
        coord.readCmdArgs(args);
        try (FileInputStream fis = new FileInputStream(coord.populateWorkload)) {
            coord.props.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (FileInputStream fis = new FileInputStream(coord.workload)) {
            coord.coreProps.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // makedir
        String dirPath = "./" + coord.directory;
        File directory = new File(dirPath);

        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("Directory created: " + dirPath);
            } else {
                System.out.println("Failed to create directory.");
            }
        } else {
            System.out.println("Directory already exists.");
        }
        coord.isWrite = coord.ifWriteWorkload();
        System.out.println("Coord : " +  coord);
        if(!coord.isWrite){
            // if is not write work load, load and warmup once
            if(coord.doLoad){
//                coord.clearDBFDBManner();
//                try (Scanner scanner = new Scanner(System.in)) { // 使用 try-with-resources 确保 Scanner 关闭
//                    if (scanner.hasNextLine()) {
//                        String line = scanner.nextLine();
//                    } else {
//                        System.err.println("No More Lines");
//                    }
//                }

                Process loadProcess = coord.loadDB();

                String bgLoadLog = coord.watchProcessOutput(loadProcess,
                        "SHUTDOWN!!!",
                        "mainclass");

                coord.saveToFile(directory+"/BGMainLoad-" + "0" +".log", bgLoadLog);
            }
            if(coord.doWarmup){
                coord.warmUp(0);
            }
        }

        if(coord.objective.equals("socialites")){
            int res = coord.runBinarySearch();
            System.out.println("Result: " + res);
        }
        else if(coord.objective.equals("soar")) {
            int res = coord.findMaxThroughput(coord.minimum);
            System.out.println("Result: " + res);
        }else{
            System.out.println("Do not support input objective");
        }


        System.exit(0);
    }

    private Stat probe(int threads, int run) {
        try {
            double tp = measureThroughput(threads, run);
            boolean sla = checkSLA(run);
            System.out.println("threads: " + threads + " ," + " count: "+ run + " SLA: " + sla);
            return new Stat(tp, sla);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int findMaxThroughput(int startThreads) throws Exception {
        final int MAX_THREADS = 65_536;     // 保护上限
        int runs = 0;                       // 实验轮计数

        /* ---------- 1) 指数扩张 ---------- */
        int lastGoodThreads = startThreads;
        Stat lastGood       = probe(startThreads, runs++);

        int rightThreads    = startThreads * 2;
        Stat right          = probe(rightThreads, runs++);

        // 持续放大：吞吐在涨 且 SLA 仍 pass
        while (right.sla && right.tp > lastGood.tp && rightThreads < MAX_THREADS) {
            lastGoodThreads = rightThreads;
            lastGood        = right;

            rightThreads   *= 2;
            right           = probe(rightThreads, runs++);
        }

        /* ---------- 2) 右边界回退到最近 pass ---------- */
        while (!right.sla) {
            int gap = rightThreads - lastGoodThreads;

            if (gap <= 1) {                // 已经贴到左端还是 fail
                return lastGoodThreads;    // 直接返回最大且 SLA=pass 的点
            }
            rightThreads = lastGoodThreads + gap / 2;  // 正常二分
            right        = probe(rightThreads, runs++);
        }

        int left  = lastGoodThreads;
        int rightT = rightThreads;                 // 区间两端均满足 SLA

        /* ---------- 3) 带约束三分搜索 ---------- */
        return constrainedTernarySearch(left, rightT, runs);
    }

    private double parseValueFromFile(File file, String prefix, String errorMsgPrefix) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(prefix)) {
                    int index = line.indexOf(prefix) + prefix.length();
                    String numStr = line.substring(index).trim();
                    return Double.parseDouble(numStr);
                }
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println(errorMsgPrefix + e.getMessage());
        }
        return -1;
    }

    public double measureThroughput(int threadcount, int count) throws Exception {
        if(doMonitor){
            String startMark = String.format("=== START TEST iteration=%d, threadCount=%d ===", count, threadcount);
        }

        startClient(threadcount, count);
        String throughputPrefix = "OVERALLTHROUGHPUT(SESSIONS/SECS):";
//        File bgLog = new File(directory, "BGMainClass-" + count + ".log");
        final String regex = "^BGMainClass-.*" + Pattern.quote(String.valueOf(count)) + Pattern.quote(".log") + "$";
        final Pattern fileNamePattern = Pattern.compile(regex);
        File directoryy = new File(directory);
        FilenameFilter logFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return fileNamePattern.matcher(name).matches();
            }
        };
        File[] logFiles = directoryy.listFiles(logFilter);
        System.out.println("Find " + logFiles.length + " Matches log files");
        double totalThroughput = 0;
        for (File bgLog : logFiles) {
            System.out.println("Processing: " + bgLog.getName());
            double throughput = parseValueFromFile(
                    bgLog,
                    throughputPrefix,
                    "Error reading BG log: " // 错误消息前缀
            );
            totalThroughput += throughput;
        }
        System.out.println("Total throughput: " + totalThroughput);

        return totalThroughput;
    }

    private int constrainedTernarySearch(int l, int r, int runs) throws Exception {
        Map<Integer, Stat> cache = new HashMap<>();   // 避免重复测同一线程数

        while (r - l > 4) {                           // 区间>4 时继续分割
            int m1 = l + (r - l) / 3;
            int m2 = r - (r - l) / 3;

            Stat s1 = cache.computeIfAbsent(m1, t -> probe(t, runs + cache.size()));
            Stat s2 = cache.computeIfAbsent(m2, t -> probe(t, runs + cache.size()));

            /* 先处理不满足 SLA 的情况 */
            if (!s2.sla) {            // 右 1/3 不合格，整体右移没意义
                r = m2 - 1;
                continue;
            }
            if (!s1.sla) {            // 左 1/3 不合格，只能向右
                l = m1 + 1;
                continue;
            }
            /* 两点都 pass：按吞吐量正常三分 */
            if (s1.tp < s2.tp) l = m1 + 1;
            else               r = m2 - 1;
        }

        /* ---------- 4) 枚举剩余极小区间 ---------- */
        double bestTp = -1;
        int    bestTh = l;
        for (int t = l; t <= r; t++) {
            Stat s = cache.computeIfAbsent(t, x -> probe(x, runs + cache.size()));
            if (s.sla && s.tp > bestTp) {
                bestTp = s.tp;
                bestTh = t;
            }
        }
        return bestTh;      // 一定满足 SLA
    }

    public boolean checkSLA(int count) {
        String satisLinePrefix = "[SatisfyingPerc] ";
        String staleLinePrefix = "[OVERALL], Staleness(staleReads/totalReads), ";
        String throughputPrefix = "OVERALLTHROUGHPUT(SESSIONS/SECS):";

//        File bgLog = new File(directory, "BGMainClass-" + count + ".log");
//        double satisPerc = parseValueFromFile(
//                bgLog,
//                satisLinePrefix,
//                "Error reading satisPerc BG log: "
//        );
        final String regex = "^BGMainClass-.*" + Pattern.quote(String.valueOf(count)) + Pattern.quote(".log") + "$";
        final Pattern fileNamePattern = Pattern.compile(regex);
        File directoryy = new File(directory);
        FilenameFilter logFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return fileNamePattern.matcher(name).matches();
            }
        };
        File[] logFiles = directoryy.listFiles(logFilter);
        System.out.println("Find " + logFiles.length + " Matches log files");
        double totalThroughput = 0;
        double totalSLA = 0;
        for (File bgLog : logFiles) {
            System.out.println("Processing: " + bgLog.getName());
            double satisPerc = parseValueFromFile(
                bgLog,
                satisLinePrefix,
                "Error reading satisPerc BG log: "
            );
            double throughput = parseValueFromFile(
                    bgLog,
                    throughputPrefix,
                    "Error reading BG log: " // 错误消息前缀
            );
            totalThroughput += throughput;
            totalSLA += satisPerc * throughput;
        }
        totalSLA = totalSLA / totalThroughput;

//        double satisPerc = 0;
        System.out.println("satisPerc read from BGMainClass-" + count + " is: " + totalSLA);

        if (totalSLA < 0) {
            return false;
        }

//        if (validation) {
//            File valLog = new File(directory, "BGMainClass-" + count + ".log");
//            double staleDataPerc = parseValueFromFile(
//                    valLog,
//                    staleLinePrefix,
//                    "Error reading BG log: " + count + " "
//            );
//            System.out.println("Staleness read from BGMainClass-" + count + " is: " + staleDataPerc);
//
//            if (staleDataPerc < 0) {
//                return false;
//            }
//            return (satisPerc >= perc && staleDataPerc <= staleness);
//        }

        return (totalSLA >= perc);
    }


    public void warmUp(int count) throws IOException {
        int userCount;
        int friendCount;
        int maxExeTime = 0;
        String warmUpWorkload = "";
        String userCountStr = props.getProperty("usercount");
        String friendCountStr = props.getProperty("friendcountperuser");
        if (userCountStr == null || friendCountStr == null) {
            throw new IllegalArgumentException("workload does not find usercount");
        }
        try {
            userCount = Integer.parseInt(userCountStr.trim());
            friendCount = Integer.parseInt(friendCountStr.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("usercount or friendcount is illegal", e);
        }

        switch (userCount) {
            case 1000:
                if(friendCount == 10){
                    maxExeTime = 20;
                } else {
                    maxExeTime = 120;
                }
                warmUpWorkload = "workloads/warmupWorkload1";
                break;
            case 10000:
                if(friendCount == 10){
                    maxExeTime = 300;
                } else {
                    maxExeTime = 600;
                }
                warmUpWorkload = "workloads/warmupWorkload2";
                break;
            case 100000:
                if(friendCount == 10){
                    maxExeTime = 600;
                } else {
                    maxExeTime = 2400;
                }
                warmUpWorkload = "workloads/warmupWorkload3";
                break;
        }
        System.out.println("WarmUp for " + maxExeTime + " seconds...");
        Process bgProcess = startBGMainClass(10, maxExeTime, warmUpWorkload, "");

        String bgLog = watchProcessOutput(bgProcess,
                "Stop requested for workload. Now Joining!",
                "mainclass");

        saveToFile(directory+"/BGMainClass-warmup" + count + ".log", bgLog);

    }


    private static double simulatePerformance(int threads) {
        return -Math.pow(threads - 50, 2) + 2500;
    }

    private boolean runSingleTest(int iteration, int threadCount) throws Exception {
        startClient(threadCount, iteration);

        boolean slaMet = checkSLA(iteration);
        return slaMet;
    }

    public int runBinarySearch() throws Exception {
        int current = minimum;
        int bestValid = -1;
        int count = 0;

        // Phase 1: Exponential search until SLA fails
        while (true) {
            boolean slaMet = runSingleTest(count, current);
            count++;
            if (slaMet) {
                bestValid = current;
                current *= 2; // Double the number of threads
            } else {
                break; // Start binary search
            }
        }

        int left = bestValid;
        int right = current - 1;

        // Phase 2: Binary search between bestValid and current-1
        while (left <= right) {
            int mid = (left + right) / 2;
            boolean slaMet = runSingleTest(count, mid);

            if (slaMet) {
                bestValid = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
            count++;
        }

        return bestValid;
    }

//    private String[] clientIPs = new String[] {"10.199.64.85", "10.222.241.59", "10.222.253.221", "10.222.253.247"};
// private String[] clientIPs = new String[] {"yiming-client-0.yiming-client", "yiming-client-1.yiming-client",
//         "yiming-client-2.yiming-client", "yiming-client-3.yiming-client", "yiming-client-4.yiming-client",
//         "yiming-client-5.yiming-client", "yiming-client-6.yiming-client", "yiming-client-7.yiming-client",
//         "yiming-client-8.yiming-client", "yiming-client-9.yiming-client"};

    private final String[] clientIPs = new String[] { "localhost" }; // Changed by SS
    public void startClient(int threads, int count) throws Exception {
        // run pipeline, clear logfiles -> clear DB -> loadDB -> issue queries -> validation(optional)
        clearLogFiles();
        System.out.println("files cleared");
        if(isWrite){
            // if it's write workload, do load and warmup each time
            if(doLoad) {
                clearDBFDBManner();
                Process loadProcess = loadDB();

                String bgLoadLog = watchProcessOutput(loadProcess,
                        "SHUTDOWN!!!",
                        "mainclass");

                saveToFile(directory+"/BGMainLoad-" + count +".log", bgLoadLog);
            }

            if(doWarmup){
                warmUp(count);
            }
        }

        System.out.println("Distributing " + threads + " threads across " + clientIPs.length + " clients for run " + count);
        int numClients = clientIPs.length;
        int threadsPerClient = (numClients > 0) ? threads / numClients : 0;
        int remainingThreads = (numClients > 0) ? threads % numClients : 0;
        ExecutorService executorService = Executors.newFixedThreadPool(numClients > 0 ? numClients : 1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
            String clientIp = clientIPs[i];
            int currentClientThreads = threadsPerClient + (i < remainingThreads ? 1 : 0);
            final int finalClientThreads = currentClientThreads;
            final String finalClientIp = clientIp;
            String clientOutputFileName = String.format("%s/BGMainClass-client-%s-run-%d.log",
                    directory,
                    finalClientIp.replace(".", "-"),
                    count);
            Runnable task = () -> {
                try {
                    Process bgProcess = startBGMainClass(finalClientThreads, duration, workload, finalClientIp);
                    String bgLog = watchProcessOutput(bgProcess,
                            "Stop requested for workload. Now Joining!",
                            "mainclass-client-" + clientIp.replace(".", "-")); // 简化进程名
                    saveToFile(clientOutputFileName, bgLog);
                } catch (IOException e) {
                    System.out.println("Error writing " + clientIp + " to " + clientOutputFileName);
                }
            };
            futures.add(executorService.submit(task));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            }  catch (InterruptedException | ExecutionException e) {
                System.out.println("Interrupted while waiting for future to finish");
            }
        }
        executorService.shutdownNow();



//        Process bgProcess = startBGMainClass(threads, duration, workload);

//        String bgLog = watchProcessOutput(bgProcess,
//                "Stop requested for workload. Now Joining!",
//                "mainclass");

//        saveToFile(directory+"/BGMainClass-" + count +".log", bgLog);
    }



    private Process startBGMainClass(int threads, int maxExeTime, String workload, String clientIP) throws IOException {
        List<String> commands = new ArrayList<>();
        commands.add("java");
        commands.add("-Xms18000m");                     // Initial heap size ~14GB
        commands.add("-Xmx18000m");                     // Maximum heap size ~14GB
//        commands.add("-XX:ActiveProcessorCount=30");
        commands.add("-XX:+UnlockExperimentalVMOptions"); // Needed for some subsequent options
        commands.add("-XX:+UseG1GC");                   // Use G1 Garbage Collector
        commands.add("-XX:MaxGCPauseMillis=75");        // Target max GC pause
        commands.add("-XX:ParallelGCThreads=15");        // Parallel GC threads (Note: See comment below)
        commands.add("-XX:ConcGCThreads=10");            // Concurrent GC threads (Note: See comment below)
        commands.add("-XX:G1NewSizePercent=10");        // G1 Young Gen initial size
        commands.add("-XX:G1MaxNewSizePercent=30");     // G1 Young Gen max size
        commands.add("-XX:ActiveProcessorCount=14");
        commands.add("-Dlogback.configurationFile=/work/conf/logback.xml");

        commands.add("-cp");
        commands.add("target/classes:target/lib/*");
        commands.add("edu.usc.bg.BGMainClass");

        commands.add("onetime");
        commands.add("-t");
        commands.add("edu.usc.bg.workloads.CoreWorkLoad");
        commands.add("-threads");
        commands.add(String.valueOf(threads));
        commands.add("-db");
        commands.add("JanusGraph.src.janusgraph.JanusGraphClient");
        commands.add("-janusGraphIp");
        commands.add("10.10.1.1"); // Changed by SS
        commands.add("-P");
        commands.add(workload);
        commands.add("-doCache");
        commands.add(String.valueOf(doCache));
        commands.add("-latency");
        commands.add(String.valueOf(latency));
        commands.add("-maxexecutiontime");
        commands.add(String.valueOf(maxExeTime));
        commands.add("-s");
        commands.add("true");

        boolean localExecution = (clientIP == null || clientIP.isEmpty() ||
                clientIP.equalsIgnoreCase("localhost") ||
                clientIP.equals("127.0.0.1"));
        if (localExecution) {
            System.out.println("Executing BGMainClass locally: " + String.join(" ", commands));
            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.redirectErrorStream(true); // Merge stdout and stderr of the process
            return pb.start();
        } else {
            try {
                return SSHExecutor.startRemoteCommand("root", clientIP, commands);
            } catch (UnsupportedOperationException e) {
                System.err.println("FATAL: SSHExecutor.startRemoteCommand is not implemented. Remote execution unavailable. " + e.getMessage());
                throw new IOException("Remote execution failed due to missing SSHExecutor.startRemoteCommand implementation.", e);
            } catch (IOException e) {
                System.err.println("Failed to start remote command on " + clientIP + ": " + e.getMessage());
                e.printStackTrace();
                throw e; // Re-throw to allow calling method to handle
            }

        }
         //SSH connect to client IP, return pb.



    }

    private void loadDBFDBManner() {


    }

    private Process loadDB() throws IOException {
        System.out.println("Reached loaddb");
        List<String> commands = new ArrayList<>();
        commands.add("java");
        commands.add("-Xms18000m");                     // Initial heap size ~14GB
        commands.add("-Xmx18000m");                     // Maximum heap size ~14GB
//        commands.add("-XX:ActiveProcessorCount=30");
        commands.add("-XX:+UnlockExperimentalVMOptions"); // Needed for some subsequent options
        commands.add("-XX:+UseG1GC");                   // Use G1 Garbage Collector
        commands.add("-XX:MaxGCPauseMillis=75");        // Target max GC pause
        commands.add("-XX:ParallelGCThreads=12");        // Parallel GC threads (Note: See comment below)
        commands.add("-XX:ConcGCThreads=10");            // Concurrent GC threads (Note: See comment below)
        commands.add("-XX:G1NewSizePercent=10");        // G1 Young Gen initial size
        commands.add("-XX:G1MaxNewSizePercent=30");     // G1 Young Gen max size
        commands.add("-XX:ActiveProcessorCount=14");
        commands.add("-Dlogback.configurationFile=/data/bg/conf/logback.xml");

        commands.add("-cp");
        commands.add("target/classes:target/lib/*");
        commands.add("edu.usc.bg.BGMainClass");

        commands.add("onetime");
        commands.add("-load");
        commands.add("edu.usc.bg.workloads.UserWorkLoad");
        commands.add("-threads");
        commands.add("10");
        commands.add("-janusGraphIp");
        commands.add("10.10.1.1"); // Changed by SS
        commands.add("-db");
        commands.add("JanusGraph.src.janusgraph.JanusGraphClient");
        commands.add("-P");
        commands.add(populateWorkload);
        commands.add("-s");
        commands.add("true");

        System.out.println("Command formed : " + commands);

        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);

        return pb.start();
    }

    private void saveToFile(String fileName, String content) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(fileName))) {
            pw.print(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private String watchProcessOutput(Process process, String keywords, String threadName) throws IOException {
        StringBuilder sb = new StringBuilder();

        Pattern numericPattern = Pattern.compile("^\\d+\\s*,\\s*\\d+$", Pattern.MULTILINE);
        InputStream combinedStream = new SequenceInputStream(
                process.getInputStream(), process.getErrorStream()
        );

        try (BufferedReader br = new BufferedReader(new InputStreamReader(combinedStream))) {

            String line;
            boolean running = true;

            while (running && (line = br.readLine()) != null) {
                boolean isNumericStat = numericPattern.matcher(line).matches();
                if(isNumericStat){
                    continue;
                }
                sb.append(line).append("\n");
                System.out.println("[process output] " + line);

                if (line.contains(keywords)) {
                    System.out.println("[detect the line] " + keywords);
                    process.destroyForcibly();
                    running = false;
                }

            }
        }

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }



    private void clearLogFiles() {
        String directory = "./";
        File dir = new File(directory);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Directory " + directory + " does not exist or is not a directory.");
            return;
        }
        File[] files = dir.listFiles();
        System.out.println("files:");
        System.out.println(Arrays.toString(files));
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                if ((name.startsWith("read") && name.endsWith(".txt"))
                        || (name.startsWith("update") && name.endsWith(".txt"))) {
                    if (file.delete()) {
                        System.out.println("Deleted file: " + file.getAbsolutePath());
                    } else {
                        System.err.println("Failed to delete file: " + file.getAbsolutePath());
                    }
                }
            }
        }
    }

    public void clearDB() {
        TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .addRegistry(JanusGraphIoRegistry.instance())
                .create();
        Cluster cluster = Cluster.build()
                .addContactPoint(janusGraphIp)
                .port(8182)
                .minConnectionPoolSize(10)
                .maxConnectionPoolSize(100)
                .maxSimultaneousUsagePerConnection(48)
                .maxWaitForConnection(5000)
                .serializer(new GraphBinaryMessageSerializerV1(registry))
                .maxContentLength(524288)
                .create();
        Client client = cluster.connect();
        // clear everything
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster))) {
            int batchSize = 50;
            int totalDeleted = 0;
            int retryLimit = 50;

            while (true) {
                boolean deletedSomething = false;
                for (int attempt = 0; attempt < retryLimit; attempt++) {
                    try {
                        g.V().limit(batchSize).drop().iterate();
                        totalDeleted += batchSize;
                        deletedSomething = true;
                        System.out.println("Deleted batch of " + batchSize);
                        break; // success
                    } catch (Exception e) {
                        System.err.println("Drop batch failed, attempt " + (attempt + 1) + ": " + e.getMessage());
                        Thread.sleep(200); // back off
                    }
                }
                if (!deletedSomething) {
                    System.err.println("Batch delete failed after retries. Aborting.");
                    break;
                }

                if (!g.V().hasNext()) {
                    break;
                }
            }

            System.out.println("Database cleared. Total deleted estimate: " + totalDeleted);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
            cluster.close();
        }
    }

    public void clearDBFDBManner() {
    }


    public void readCmdArgs(String[] args) {
        /**
         * -workload: specified workload, should be a file
         * -populateWorkload: specified populateWorkload, should be a file
         * -latency: response time in milliseconds
         * -staleness: percentage of stale data
         * -duration: duration in milliseconds
         * -directory: log directory name
         * -minimum: min threads
         * -maximum: maxi threads
         * -objective: rate soar or socialites, "soar" means rate soar, "socialites" means rate socialites
         * -validation: true or false, do validation or not
         */
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-doLoad":
                    if (i + 1 < args.length) {
                        doLoad = Boolean.parseBoolean(args[++i]);
                    } else {
                        System.err.println("Missing value for -doLoad");
                        System.exit(1);
                    }
                    break;
                case "-janusGraphIp":
                    if (i + 1 < args.length) {
                        janusGraphIp = args[++i];
                    } else {
                        System.err.println("Missing value for -doLoad");
                        System.exit(1);
                    }
                    break;
                case "-doMonitor":
                    if (i + 1 < args.length) {
                        doMonitor = Boolean.parseBoolean(args[++i]);
                    } else {
                        System.err.println("Missing value for -doLoad");
                        System.exit(1);
                    }
                    break;
                case "-doCache":
                    if (i + 1 < args.length) {
                        doCache = Boolean.parseBoolean(args[++i]);
                    } else {
                        System.err.println("Missing value for -doCache");
                        System.exit(1);
                    }
                    break;
                case "-doWarmup":
                    if (i + 1 < args.length) {
                        doWarmup = Boolean.parseBoolean(args[++i]);
                    } else {
                        System.err.println("Missing value for -doWarmup");
                        System.exit(1);
                    }
                    break;
                case "-workload":
                    if (i + 1 < args.length) {
                        workload = args[++i];
                    } else {
                        System.err.println("Missing value for -workload");
                        System.exit(1);
                    }
                    break;
                case "-populateWorkload":
                    if (i + 1 < args.length) {
                        populateWorkload = args[++i];
                    } else {
                        System.err.println("Missing value for -populateWorkload");
                        System.exit(1);
                    }
                    break;
                case "-latency":
                    if (i + 1 < args.length) {
                        latency = Double.parseDouble(args[++i]);
                    } else {
                        System.err.println("Missing value for -latency");
                        System.exit(1);
                    }
                    break;
                case "-perc":
                    if (i + 1 < args.length) {
                        perc = Double.parseDouble(args[++i]);
                    } else {
                        System.err.println("Missing value for -perc");
                        System.exit(1);
                    }
                    break;
                case "-staleness":
                    if (i + 1 < args.length) {
                        staleness = Double.parseDouble(args[++i]);
                    } else {
                        System.err.println("Missing value for -staleness");
                        System.exit(1);
                    }
                    break;
                case "-duration":
                    if (i + 1 < args.length) {
                        duration = Integer.parseInt(args[++i]);
                    } else {
                        System.err.println("Missing value for -duration");
                        System.exit(1);
                    }
                    break;
                case "-directory":
                    if (i + 1 < args.length) {
                        directory = args[++i];
                    } else {
                        System.err.println("Missing value for -directory");
                        System.exit(1);
                    }
                    break;
                case "-minimum":
                    if (i + 1 < args.length) {
                        minimum = Integer.parseInt(args[++i]);
                    } else {
                        System.err.println("Missing value for -minimum");
                        System.exit(1);
                    }
                    break;
                case "-objective":
                    if (i + 1 < args.length) {
                        objective = args[++i].toLowerCase();
                        if (!objective.equals("soar") && !objective.equals("socialites")) {
                            System.err.println("Invalid value for -objective. Must be 'soar' or 'socialites'.");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Missing value for -objective");
                        System.exit(1);
                    }
                    break;
                case "-validation":
                    if (i + 1 < args.length) {
                        String val = args[++i].toLowerCase();
                        if (!val.equals("true") && !val.equals("false")) {
                            System.err.println("Invalid value for -validation. Must be 'true' or 'false'.");
                            System.exit(1);
                        }
                        validation = Boolean.parseBoolean(val);
                    } else {
                        System.err.println("Missing value for -validation");
                        System.exit(1);
                    }
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    break;
            }
        }
    }
}