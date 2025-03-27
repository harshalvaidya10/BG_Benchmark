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

    private static final String SERVER_ADDRESS = "127.0.0.1";
    private static final int SERVER_PORT = 10655;

    private static final long INTERVAL_MILLIS = 60 * 1000;

    private static final String MESSAGE = "GetData";

    public static void main(String[] args) {
        while (true) {
            // 1. 建立 Socket 连接
            try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                 OutputStream os = socket.getOutputStream()) {

                // 2. 组装并发送数据包：[4字节长度(小端)] + [实际内容]
                byte[] msgBytes = MESSAGE.getBytes();
                int msgLength = msgBytes.length;
                byte[] lenBytes = ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(msgLength)
                        .array();

                os.write(lenBytes);
                os.write(msgBytes);
                os.flush();

                // 3. 连接使用完后，自动关闭（try-with-resources 会自动 close）

            } catch (IOException e) {
                System.err.println("Error connecting or sending data: " + e.getMessage());
                // 若需重试机制，可自行在这里加逻辑
            }

            // 4. 等待指定间隔
            try {
                Thread.sleep(INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 恢复中断标记
                System.err.println("Thread was interrupted. Exiting.");
                break;
            }
        }
    }
}
