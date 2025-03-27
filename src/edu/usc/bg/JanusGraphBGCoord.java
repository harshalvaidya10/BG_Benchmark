/**
 * @description BGCoord for janusgraph
 * @date 2025/3/27 11:02
 * @version 1.0
 */

package edu.usc.bg;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JanusGraphBGCoord {

    private String workload;
    private double latency;
    private double staleness;
    private int duration;
    private String directory;
    private int minimum;
    private int maximum;
    private String objective;
    private boolean validation;

    public static void main(String[] args) throws IOException, InterruptedException {
        JanusGraphBGCoord coord = new JanusGraphBGCoord();
        coord.readCmdArgs(args);
        coord.runBinarySearch();
        coord.startClient(10);
    }


    public boolean checkSLA(int mid){
        return true;
    }

    private double simulatePerformance(int threads) {
        return -Math.pow(threads - 50, 2) + 2500;
    }

    public int runBinarySearch() {
        int left = minimum;
        int right = maximum;
        int bestValid = -1;

        while (left <= right) {
            int mid = (left + right) / 2;
            System.out.println("Testing, number of threads: T = " + mid);

            // startClient(mid);
            boolean slaMet = checkSLA(mid);
            double performance = simulatePerformance(mid);
            System.out.println("threadcount = " + mid +
                    ",performance = " + performance +
                    ", SLA " + (slaMet ? "meet" : "not meet"));

            if (slaMet) {
                // if meet certain sla:
                bestValid = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return bestValid;
    }

    public void startClient(int threads) throws IOException, InterruptedException {
        clearLogFiles();
        Process bgProcess = startBGMainClass(threads);

        String bgLog = watchProcessOutput(bgProcess,
                "Visualization thread has Stopped...",
                "BGMainClass");

        saveToFile("BGMainClass.log", bgLog);

        if (validation) {
            Process validationProcess = startValidationMainClass(threads);

            String validationLog = watchProcessOutput(validationProcess,
                    " of reads observed the value of ",
                    "Data was stale...",
                    "ValidationMainClass");

            saveToFile("ValidationMainClass.log", validationLog);
        }
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
        commands.add("-s");
        commands.add("true");

        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);

        return pb.start();
    }

    private Process startValidationMainClass(int threads) throws IOException {
        List<String> commands = new ArrayList<>();
        commands.add("java");
        commands.add("-cp");
        commands.add("build/classes:lib/*");
        commands.add("edu.usc.bg.validator.ValidationMainClass");

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


    private String watchProcessOutput(Process process, String... keywords) throws IOException {
        StringBuilder sb = new StringBuilder();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {

            String line;
            boolean running = true;

            while (running && (line = br.readLine()) != null) {
                sb.append(line).append("\n");
                System.out.println("[process output] " + line); // 也可注释掉

                for (String kw : keywords) {
                    if (line.contains(kw)) {
                        System.out.println("[detect the line] " + kw
                                + " => interrupt the process...");
                        process.destroyForcibly();
                        running = false;
                        break;
                    }
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

    public void readCmdArgs(String[] args) {
        /**
         * -workload: specified workload, should be a file
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
                case "-latency":
                    if (i + 1 < args.length) {
                        latency = Double.parseDouble(args[++i]);
                    } else {
                        System.err.println("Missing value for -latency");
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
                case "-maximum":
                    if (i + 1 < args.length) {
                        maximum = Integer.parseInt(args[++i]);
                    } else {
                        System.err.println("Missing value for -maximum");
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
