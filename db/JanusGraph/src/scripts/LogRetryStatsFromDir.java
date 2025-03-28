/**
 * @description
 * @date 2025/3/28 12:22
 * @version 1.0
 */

package scripts;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class LogRetryStatsFromDir {

    public static class RetryStats {
        int numExceptions = 0;
        boolean failed = false;
        String filename;

        RetryStats(String filename) {
            this.filename = filename;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java LogRetryStatsFromDir <logDirectory>");
            return;
        }

        String logDirPath = args[0];
        File logDir = new File(logDirPath);
        if (!logDir.exists() || !logDir.isDirectory()) {
            System.err.println("Invalid directory: " + logDirPath);
            return;
        }

        // All operation stats
        Map<String, RetryStats> allStats = new LinkedHashMap<>();

        // Track max per file
        Map<String, Integer> fileMaxExceptions = new HashMap<>();
        Map<String, Boolean> fileHasFailure = new HashMap<>();

        Pattern exceptionPattern = Pattern.compile("\\[Operation ([^\\]]+)] \\[Thread \\d+] Attempt (\\d+)/(\\d+) failed:");

        File[] logFiles = logDir.listFiles((dir, name) -> name.startsWith("BGMainClass") && name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            System.out.println("No matching log files found in directory.");
            return;
        }

        for (File logFile : logFiles) {
            String filename = logFile.getName();
            Map<String, RetryStats> fileStats = new HashMap<>();

            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher matcher = exceptionPattern.matcher(line);
                    if (matcher.find()) {
                        String operationId = matcher.group(1);
                        int attempt = Integer.parseInt(matcher.group(2));
                        int maxRetries = Integer.parseInt(matcher.group(3));

                        fileStats.putIfAbsent(operationId, new RetryStats(filename));
                        RetryStats stats = fileStats.get(operationId);
                        stats.numExceptions++;

                        if (attempt == maxRetries) {
                            stats.failed = true;
                        }
                    }
                }
            }

            int maxExceptionInFile = 0;
            boolean hasFailedOp = false;

            // Merge fileStats into allStats
            for (Map.Entry<String, RetryStats> entry : fileStats.entrySet()) {
                String opId = entry.getKey();
                RetryStats stats = entry.getValue();

                if (stats.failed) {
                    stats.numExceptions = 0; // per your rule
                }

                allStats.put(opId, stats);

                maxExceptionInFile = Math.max(maxExceptionInFile, stats.numExceptions);
                if (stats.failed) {
                    hasFailedOp = true;
                }
            }

            fileMaxExceptions.put(filename, maxExceptionInFile);
            fileHasFailure.put(filename, hasFailedOp);
        }

        // Output
        System.out.println("filename,operation_id,num_exceptions,failed_retries");

        for (Map.Entry<String, RetryStats> entry : allStats.entrySet()) {
            RetryStats stats = entry.getValue();
            int failedFlag = stats.failed ? 1 : 0;
            System.out.printf("%s,%s,%d,%d%n", stats.filename, entry.getKey(), stats.numExceptions, failedFlag);
        }

        // Summary
        System.out.println("\n--- File Summary ---");
        System.out.println("filename,max_num_exceptions,has_failed_retries");
        for (String filename : fileMaxExceptions.keySet()) {
            int maxExc = fileMaxExceptions.get(filename);
            int failed = fileHasFailure.getOrDefault(filename, false) ? 1 : 0;
            System.out.printf("%s,%d,%d%n", filename, maxExc, failed);
        }
    }
}
