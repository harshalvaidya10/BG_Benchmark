package edu.usc.bg;

import java.io.IOException;
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

        int exitCode = process.waitFor();
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
    public static void logToMachine(String machine, String message) throws Exception {
        String remoteUser = "Ziqif";
        String identityFile = "/users/Ziqif/.ssh/id_rsa";
        switch (machine) {
            case "node3":
                runRemoteCmd("apt066.apt.emulab.net", remoteUser, identityFile,
                        "echo \"" + message + "\" >> /users/Ziqif/monitor.log");
                break;
            case "node4":
                runRemoteCmd("apt075.apt.emulab.net", remoteUser, identityFile,
                        "echo \"" + message + "\" >> /users/Ziqif/monitor.log");
                break;
            case "node5":
                writeLocalLog("/users/Ziqif/monitor.log", message);
                break;
            default:
                throw new IllegalArgumentException("Unknown machine: " + machine);
        }
    }

    public static void logToAllNodes(String message) throws Exception {
        logToMachine("node3", message);
        logToMachine("node4", message);
        logToMachine("node5", message);
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
