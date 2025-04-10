package edu.usc.bg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class SSHExecutor {

    /**
     * @param remoteHost
     * @param remoteUser
     * @param command
     * @throws IOException
     * @throws InterruptedException
     */
    public static void runRemoteCmd(String remoteHost,
                                    String remoteUser,
                                    String identityFile,
                                    String command)
            throws IOException, InterruptedException {
        // e.g. ssh -o StrictHostKeyChecking=no -i /home/myuser/.ssh/id_rsa user@host "echo 'test' >> /tmp/test.log"
        String sshCommand = String.format(
                "ssh -o StrictHostKeyChecking=no -i %s %s@%s \"%s\"",
                identityFile, remoteUser, remoteHost, command
        );

        System.out.println("Executing: " + sshCommand);

        ProcessBuilder builder = new ProcessBuilder("bash", "-c", sshCommand);
        builder.redirectErrorStream(true);
        Process process = builder.start();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[SSH Output] " + line);
            }
        }
        int exitCode = process.waitFor();
        System.out.println("SSH exit code: " + exitCode);
        if (exitCode != 0) {
            throw new RuntimeException("SSH command failed with exit code " + exitCode);
        }
    }

    public static void writeLocalLog(String localFile, String message) throws IOException {
        String line = message + "\n";
        Files.write(
                Paths.get(localFile),
                line.getBytes(),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    /**
     *
     * @param machine  比如 "node3", "node4", "node5"
     * @param message  要写的文本
     * @throws Exception
     */
    public static void logToMachine(String machine, String prefix, String message) throws Exception {
        String remoteUser = "Ziqif";
        String identityFile = "/users/Ziqif/.ssh/id_rsa";
        // 根据 prefix 构造目标日志文件路径
        String targetLog = "/users/Ziqif/" + prefix + "_monitor.log";
        switch (machine) {
            case "node3":
                runRemoteCmd("apt066.apt.emulab.net", remoteUser, identityFile,
                        "echo '" + message + "' >> " + targetLog);
                break;
            case "node4":
                runRemoteCmd("apt075.apt.emulab.net", remoteUser, identityFile,
                        "echo '" + message + "' >> " + targetLog);
                break;
            case "node5":
                writeLocalLog(targetLog, message);
                break;
            default:
                throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }


    public static void logToAllNodes(String prefix, String message) throws Exception {
        logToMachine("node3", prefix, message);
        logToMachine("node4", prefix, message);
        logToMachine("node5", prefix, message);
    }

    public static void startMonitoring(String machine, String prefix) throws IOException, InterruptedException {
        String remoteUser = "Ziqif";
        String identityFile = "/users/Ziqif/.ssh/id_rsa";
        switch (machine) {
            case "node3":
                runRemoteCmd("apt066.apt.emulab.net", remoteUser, identityFile,
                        "nohup /users/Ziqif/scripts/monitor_perf.sh " + prefix +
                                " > /users/Ziqif/" + prefix + "_monitor.log 2>&1 &");
                break;
            case "node4":
                runRemoteCmd("apt075.apt.emulab.net", remoteUser, identityFile,
                        "nohup /users/Ziqif/scripts/monitor_perf.sh " + prefix +
                                " > /users/Ziqif/" + prefix + "_monitor.log 2>&1 &");
                break;
            case "node5":
                startLocalMonitor(prefix);
                break;
            default:
                throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }

    private static void startLocalMonitor(String prefix) throws IOException {
        String localCmd = String.format("nohup %s %s > /users/Ziqif/%s_monitor.log 2>&1 &", "/users/Ziqif/bg_benchmark_fdb/monitor_perf.sh", prefix, prefix);
        ProcessBuilder builder = new ProcessBuilder("bash", "-c", localCmd);
        builder.start();
    }

    public static void stopMonitoring(String machine) throws IOException, InterruptedException {
        String remoteUser = "Ziqif";
        String identityFile = "/users/Ziqif/.ssh/id_rsa";
        switch (machine) {
            case "node3":
                runRemoteCmd("apt066.apt.emulab.net", remoteUser, identityFile, "pkill -f monitor_perf.sh");
                break;
            case "node4":
                runRemoteCmd("apt075.apt.emulab.net", remoteUser, identityFile, "pkill -f monitor_perf.sh");
                break;
            case "node5":
                stopLocalMonitor();
                break;
            default:
                throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }

    private static void stopLocalMonitor() throws IOException {
        String localCmd = "pkill -f monitor_perf.sh";
        ProcessBuilder builder = new ProcessBuilder("bash", "-c", localCmd);
        try {
            Process proc = builder.start();
            proc.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        String remoteUser = "Ziqif";
        String identityFile = "/users/Ziqif/.ssh/id_rsa";
        try {
            runRemoteCmd("apt066.apt.emulab.net", remoteUser, identityFile,
                    "echo \"=== START TEST iteration=1 ===\" >> /users/Ziqif/monitor.log");

            runRemoteCmd("apt075.apt.emulab.net", remoteUser, identityFile,
                    "echo \"Just a test line\" >> /users/Ziqif/test.log");

            System.out.println("Commands executed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
