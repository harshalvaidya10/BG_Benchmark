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

        Map<String, Integer> exceptionCountMap = new HashMap<>();
        Set<String> failedOperations = new HashSet<>();

        Pattern exceptionPattern = Pattern.compile("\\[Operation ([^\\]]+)] \\[Thread \\d+] Attempt (\\d+)/(\\d+) failed:");

        File[] logFiles = logDir.listFiles((dir, name) -> name.startsWith("BGMainClass") && name.endsWith(".log"));

        if (logFiles == null || logFiles.length == 0) {
            System.out.println("No matching log files found in directory.");
            return;
        }

        for (File logFile : logFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher matcher = exceptionPattern.matcher(line);
                    if (matcher.find()) {
                        String operationId = matcher.group(1);
                        int attempt = Integer.parseInt(matcher.group(2));
                        int maxRetries = Integer.parseInt(matcher.group(3));

                        // +1 for every failure line
                        exceptionCountMap.put(operationId, exceptionCountMap.getOrDefault(operationId, 0) + 1);

                        // If it's the final attempt, mark as failed
                        if (attempt == maxRetries) {
                            failedOperations.add(operationId);
                        }
                    }
                }
            }
        }

        // 输出统计结果
        System.out.println("operation_id,num_exceptions,failed_retries");
        for (String opId : exceptionCountMap.keySet()) {
            int exceptions = exceptionCountMap.get(opId);
            boolean failed = failedOperations.contains(opId);

            int finalExceptions = failed ? 0 : exceptions;
            int failedFlag = failed ? 1 : 0;

            System.out.printf("%s,%d,%d%n", opId, finalExceptions, failedFlag);
        }
    }
}