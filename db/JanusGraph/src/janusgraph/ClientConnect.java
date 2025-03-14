/**
 * @description
 * @date 2025/3/14 9:59
 * @version 1.0
 */

package janusgraph;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Scanner;

public class ClientConnect {
    public static void main(String[] args) {
        String serverAddress = "128.110.96.121"; // 服务器 IP
        int port = 6001; // 服务器监听的端口

        try (Socket socket = new Socket(serverAddress, port);
             OutputStream os = socket.getOutputStream();
             InputStream is = socket.getInputStream();
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Connected to server. Type your command (or 'exit' to quit):");

            while (true) {
                System.out.print("> ");
                String msg = scanner.nextLine(); // 用户输入命令

                if (msg.equalsIgnoreCase("exit")) {
                    System.out.println("Closing connection...");
                    break;
                }

                byte[] msgBytes = msg.getBytes();
                int msgLength = msgBytes.length;
                byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(msgLength).array();


                os.write(lenBytes);
                os.write(msgBytes);
                os.flush();

//                byte[] lenResponseBytes = new byte[4];
//                is.read(lenResponseBytes);
//                int responseLength = ByteBuffer.wrap(lenResponseBytes).getInt();
//
//                // 读取实际数据（循环确保读完整）
//                byte[] responseBytes = new byte[responseLength];
//                int bytesRead = 0;
//                while (bytesRead < responseLength) {
//                    int result = is.read(responseBytes, bytesRead, responseLength - bytesRead);
//                    if (result == -1) break;
//                    bytesRead += result;
//                }
//
//                String response = new String(responseBytes); // 避免乱码
//                System.out.println("Server Response: " + response);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
