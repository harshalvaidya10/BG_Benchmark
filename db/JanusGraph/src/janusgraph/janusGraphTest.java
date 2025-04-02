/**
 * @description
 * @date 2025/2/14 8:49
 * @version 1.0
 */

package janusgraph;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.zip.DeflaterOutputStream;

public class janusGraphTest {
    private static double simulatePerformance(int threads) {
        return -Math.pow(threads - 50, 2) + 2500;
    }

    // ============= 2) measureThroughput 替代真实压测 =============
    // 实际使用时，你会在这里启动压测并解析日志，这里仅做模拟
    private double measureThroughput(int threads, int count) {
        double t = simulatePerformance(threads);
        System.out.println("threads=" + threads + " count=" + count + " -> throughput=" + t);
        return t;
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

    // ============= 4) 在 [left, right] 范围内用三分法搜索最大吞吐量 =============
    private int ternarySearchMaxThroughput(int left, int right, int count) throws Exception {
        if (left >= right) {
            throw new IllegalArgumentException("left must be strictly smaller than right");
        }

        System.out.println("=> TernarySearch range: [" + left + ", " + right + "]");

        while (right - left > 2) {
            int m1 = left + (right - left) / 3;
            int m2 = right - (right - left) / 3;

            double throughput1 = measureThroughput(m1, count);
            count++;
            double throughput2 = measureThroughput(m2, count);
            count++;

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

        System.out.println("final best threadcount = " + bestThreadCount);
        return bestThreadCount;
    }

    public static void main(String[] args) {
        try {
            janusGraphTest janusGraphTest = new janusGraphTest();
            int bestThread = janusGraphTest.findMaxThroughput(1);  // 从 1 线程开始
            System.out.println("final best threadcount = " + bestThread);

            // 对于这个模拟的抛物线，我们期待 bestThread=50
            if (bestThread == 50) {
                System.out.println("passed! threadcount=50 reached 2500");
            } else {
                System.out.println("Note: The result found by the rule of thirds may not be 50, it is actually found=" + bestThread);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
