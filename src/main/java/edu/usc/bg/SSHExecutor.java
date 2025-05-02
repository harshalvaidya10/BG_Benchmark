package edu.usc.bg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SSHExecutor {

    // ---------------- 配置常量 ----------------
    private static final String REMOTE_USER     = "Ziqif";
    private static final String IDENTITY_FILE   = "/users/Ziqif/.ssh/id_rsa";
    private static final String REMOTE_SCRIPT   = "/users/Ziqif/scripts/monitor_perf.sh";
    private static final String LOCAL_SCRIPT    = "/users/Ziqif/bg_benchmark_fdb/monitor_perf.sh";

    private static final Map<String,String> HOST_MAP;
    static {
        Map<String,String> map = new HashMap<>();
        map.put("fdbCache",     "apt068.apt.emulab.net");
        map.put("fdbStorage",   "apt071.apt.emulab.net");
        map.put("fdbLogServer", "apt069.apt.emulab.net");
        map.put("janusGraph",   "apt075.apt.emulab.net");
        HOST_MAP = Collections.unmodifiableMap(map);
    }

    public static void runRemoteCmdNonBlocking(String machine, String shellCmd)
            throws IOException {
        String host = HOST_MAP.get(machine);
        String ssh = String.format(
                "ssh -o StrictHostKeyChecking=no -i %s %s@%s \"%s\"",
                IDENTITY_FILE, REMOTE_USER, host, shellCmd
        );
        // 用 ProcessBuilder 启动，但不调用 waitFor()
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", ssh);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        // 如果你想打印一下 SSH 的输出，也可以异步读一小段然后丢弃：
        new Thread(() -> {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // 可选：System.out.println("[SSH-DBG] " + line);
                }
            } catch (IOException ignored) {}
        }).start();
        // **不** 调用 p.waitFor()，方法立即返回
    }

    private static final int SSH_TIMEOUT_SECONDS = 120;
    static void runRemoteCmd(String host, String shellCmd)
            throws IOException, InterruptedException {
        String machine = HOST_MAP.get(host);

        // 原来的 ssh 命令
        String sshCmd = String.format(
                "ssh -o StrictHostKeyChecking=no -i %s %s@%s \"%s\"",
                IDENTITY_FILE, REMOTE_USER, machine, shellCmd
        );

        // 在前面加 timeout
        String timeoutCmd = String.format(
                "timeout %d %s",
                SSH_TIMEOUT_SECONDS,
                sshCmd
        );

        // 调用超时包装后的命令
        executeLocalCommand(timeoutCmd);
    }

    private static void runLocalCmd(String shellCmd) throws IOException, InterruptedException {
        executeLocalCommand(shellCmd);
    }

    private static void executeLocalCommand(String cmd) throws IOException, InterruptedException {
        System.out.println("Running: " + cmd);
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[OUTPUT] " + line);
            }
        }
        int exit = p.waitFor();
        if (exit != 0) {
            throw new RuntimeException("Command exited with code " + exit);
        }
    }

    private static void appendLocalLog(String logFile, String msg) throws IOException {
        Files.write(Paths.get(logFile),
                (msg + "\n").getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public static void logToMachine(String machine, String prefix, String message) throws Exception {
        String logFile = String.format("%s_%s_monitor.log", machine, prefix);
        if ("bgClient".equals(machine)) {
            appendLocalLog(logFile, message);
        } else if (HOST_MAP.containsKey(machine)) {
            String cmd = String.format("echo '%s' >> %s", message.replace("'", "\\'"), logFile);
            runRemoteCmd(machine, cmd);
        } else {
            throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }

    public static void logToAllNodes(String prefix, String message) throws Exception {
        for (String machine : HOST_MAP.keySet()) {
            logToMachine(machine, prefix, message);
        }
        logToMachine("bgClient", prefix, message);
    }

    public static void deleteLogsAllNodes(String prefix) throws IOException, InterruptedException {
        String pattern = prefix + "*.log";
        String localPattern = "bg_benchmark_fdb/" + pattern;
        for (String machine : HOST_MAP.keySet()) {
            String cmd  = String.format("rm -f %s", pattern);
            System.out.println("Deleting logs on " + machine + ": " + pattern);
            runRemoteCmd(machine, cmd);
        }
        System.out.println("Deleting logs on local (bgClient): " + localPattern);
        runLocalCmd("rm -f " + localPattern);
    }

    public static void startMonitoring(String machine, String prefix) throws Exception {
        String logFile = String.format("%s_%s_monitor.log", machine, prefix);
        String arg      = machine + "_" + prefix;
        String cmd      = String.format("nohup %s %s > %s 2>&1 &",
                REMOTE_SCRIPT, arg, logFile);

        if ("bgClient".equals(machine)) {
            String localCmd = String.format("nohup %s %s > %s 2>&1 &",
                    LOCAL_SCRIPT, arg, logFile);
            runLocalCmd(localCmd);
        } else if (HOST_MAP.containsKey(machine)) {
            runRemoteCmd(machine, cmd);
        } else {
            throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }

    public static void stopMonitoring(String machine) throws Exception {
        String killCmd = "pkill -9 -f monitor_perf.sh";
        if ("bgClient".equals(machine)) {
            runLocalCmd(killCmd);
        } else if (HOST_MAP.containsKey(machine)) {
            runRemoteCmd(machine, killCmd);
        } else {
            throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }

    public static void startAllMonitoring(String prefix) throws Exception {
        for (String m : HOST_MAP.keySet()) {
            startMonitoring(m, prefix);
        }
        startMonitoring("bgClient", prefix);
    }

    public static void stopAllMonitoring() throws Exception {
        for (String m : HOST_MAP.keySet()) {
            stopMonitoring(m);
        }
        stopMonitoring("bgClient");
    }
}
