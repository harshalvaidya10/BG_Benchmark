/**
 * @description BGCoord for janusgraph
 * @date 2025/3/27 11:02
 * @version 1.0
 */

package edu.usc.bg;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphBGCoord {

    private String workload;
    private String populateWorkload;
    private double latency;
    private double perc;
    private double staleness;
    private int duration;
    private String directory;
    private int minimum;
    private String objective;
    private boolean validation;

    public static void main(String[] args) throws Exception {
        JanusGraphBGCoord coord = new JanusGraphBGCoord();
        coord.readCmdArgs(args);
        // makedir
        String dirPath = "./"+coord.directory;
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
        try {
            System.out.println("Starting monitor scripts on node3, node4, node5...");
            SSHExecutor.startMonitoring("node3", coord.directory);
            SSHExecutor.startMonitoring("node4", coord.directory);
            SSHExecutor.startMonitoring("node5", coord.directory);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to start monitoring scripts. Continuing anyway...");
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

        try {
            System.out.println("Stopping monitor scripts on node3, node4, node5...");
            SSHExecutor.stopMonitoring("node3");
            SSHExecutor.stopMonitoring("node4");
            SSHExecutor.stopMonitoring("node5");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to stop monitor scripts.");
        }

    }

    public int findMaxThroughput(int startThreadNumber) throws Exception {
        int count = 0;
        int prevLeft = startThreadNumber;
        double prevLeftThroughput = measureThroughput(prevLeft, count);
        count++;

        int oldLeft = prevLeft;
        double oldLeftThroughput = prevLeftThroughput;

        int newRight = oldLeft * 2;
        double newRightThroughput = measureThroughput(newRight, count);
        count++;

        while (true) {
            if (newRightThroughput > oldLeftThroughput) {

                prevLeft = oldLeft;

                oldLeft = newRight;
                oldLeftThroughput = newRightThroughput;

                if (newRight >= 65536) {
                    System.out.println("Hit protection limit = 65536. Stop expansion.");
                    break;
                }

                newRight = oldLeft * 2;
                newRightThroughput = measureThroughput(newRight, count);
                count++;

            } else {
                System.out.println(
                        "Detected throughput drop (or not bigger). " +
                                "Start ternary search in [" + prevLeft + "," + newRight + "]"
                );
                return ternarySearchMaxThroughput(prevLeft, newRight, count);
            }
        }
        System.out.println(
                "Throughput never dropped or reached limit, searching final interval ["
                        + oldLeft + ", " + newRight + "]"
        );
        return ternarySearchMaxThroughput(oldLeft, newRight, count);
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
        startClient(threadcount, count);
        String throughputPrefix = "OVERALLTHROUGHPUT(SESSIONS/SECS):";
        File bgLog = new File(directory, "BGMainClass-" + count + ".log");

        double throughput = parseValueFromFile(
                bgLog,
                throughputPrefix,
                "Error reading BG log: "
        );

        System.out.println("threads=" + threadcount + " count=" + count + " -> throughput=" + throughput);
        return throughput;
    }

    public int ternarySearchMaxThroughput(int left, int right, int count) throws Exception {
        if (left >= right) {
            throw new IllegalArgumentException("left must smaller than right");
        }

        while (right - left > 2) {
            int m1 = left + (right - left) / 3;
            int m2 = right - (right - left) / 3;

            double throughput1 = measureThroughput(m1, count);
            count ++;
            double throughput2 = measureThroughput(m2, count);
            count ++;

            if (throughput1 < throughput2) {
                left = m1 + 1;
            } else {
                right = m2 - 1;
            }
        }

        int bestThreadCount = left;
        double bestThroughput = measureThroughput(left, count);
        count++;

        for (int t = left + 1; t <= right; t++) {
            double currentThroughput = measureThroughput(t, count);
            count++;
            if (currentThroughput > bestThroughput) {
                bestThroughput = currentThroughput;
                bestThreadCount = t;
            }
        }

        return bestThreadCount;
    }

    public boolean checkSLA(int count) {
        String satisLinePrefix = "[SatisfyingPerc] ";
        String staleLinePrefix = "[OVERALL], Staleness(staleReads/totalReads), ";

        File bgLog = new File(directory, "BGMainClass-" + count + ".log");
        double satisPerc = parseValueFromFile(
                bgLog,
                satisLinePrefix,
                "Error reading BG log: "
        );
        System.out.println("satisPerc read from BGMainClass-" + count + " is: " + satisPerc);

        if (satisPerc < 0) {
            return false;
        }

        if (validation) {
            File valLog = new File(directory, "BGMainClass-" + count + ".log");
            double staleDataPerc = parseValueFromFile(
                    valLog,
                    staleLinePrefix,
                    "Error reading BG log: " + count + " "
            );
            System.out.println("Staleness read from BGMainClass-" + count + " is: " + staleDataPerc);

            if (staleDataPerc < 0) {
                return false;
            }
            return (satisPerc >= perc && staleDataPerc <= staleness);
        }

        return (satisPerc >= perc);
    }



    private static double simulatePerformance(int threads) {
        return -Math.pow(threads - 50, 2) + 2500;
    }

    private boolean runSingleTest(int iteration, int threadCount) throws Exception {
        String startMark = String.format("=== START TEST iteration=%d, threadCount=%d ===", iteration, threadCount);
        SSHExecutor.logToAllNodes(directory, startMark);

        System.out.println("Testing, number of threads: T = " + threadCount);
        startClient(threadCount, iteration);

        boolean slaMet = checkSLA(iteration);
        System.out.println("threadcount = " + threadCount + ", SLA " + (slaMet ? "meet" : "not meet"));

        String endMark = String.format("=== END TEST iteration=%d, threadCount=%d ===", iteration, threadCount);
        SSHExecutor.logToAllNodes(directory, endMark);

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

    public void startClient(int threads, int count) throws Exception {
        // run pipeline, clear logfiles -> clear DB -> loadDB -> issue queries -> validation(optional)
        clearLogFiles();
        clearDB();
        Process loadProcess = loadDB();

        String bgLoadLog = watchProcessOutput(loadProcess,
                "SHUTDOWN!!!",
                "mainclass");

        saveToFile(directory+"/BGMainLoad-" + count +".log", bgLoadLog);

        Process bgProcess = startBGMainClass(threads);

        String bgLog = watchProcessOutput(bgProcess,
                "Stop requested for workload. Now Joining!",
                "mainclass");

        saveToFile(directory+"/BGMainClass-" + count +".log", bgLog);
    }

    private Process startBGMainClass(int threads) throws IOException {
        List<String> commands = new ArrayList<>();
        commands.add("java");
        commands.add("-cp");
        commands.add("build/classes:lib/*");
        commands.add("edu.usc.bg.BGMainClass");

        commands.add("onetime");
        commands.add("-t");
        commands.add("edu.usc.bg.workloads.CoreWorkLoad");
        commands.add("-threads");
        commands.add(String.valueOf(threads));
        commands.add("-db");
        commands.add("janusgraph.JanusGraphClient");
        commands.add("-P");
        commands.add(workload);
        commands.add("-latency");
        commands.add(String.valueOf(latency));
        commands.add("-maxexecutiontime");
        commands.add(String.valueOf(duration));
        commands.add("-s");
        commands.add("true");

        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);

        return pb.start();
    }

    private Process loadDB() throws IOException {
        List<String> commands = new ArrayList<>();
        commands.add("java");
        commands.add("-cp");
        commands.add("build/classes:lib/*");
        commands.add("edu.usc.bg.BGMainClass");

        commands.add("onetime");
        commands.add("-load");
        commands.add("edu.usc.bg.workloads.UserWorkLoad");
        commands.add("-threads");
        commands.add("1");
        commands.add("-db");
        commands.add("janusgraph.JanusGraphClient");
        commands.add("-P");
        commands.add(populateWorkload);
        commands.add("-s");
        commands.add("true");

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

        long startTime = System.currentTimeMillis();
        final long timeout = 3 * 60 * 1000; // 3 minutes in milliseconds
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

                // Check for timeout
                if (System.currentTimeMillis() - startTime > timeout) {
                    System.out.println("[timeout] Process exceeded 3 minutes. Forcibly terminating...");
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
                .addContactPoint("128.110.96.75")
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
            int batchSize = 500;
            int totalDeleted = 0;
            int retryLimit = 5;

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
