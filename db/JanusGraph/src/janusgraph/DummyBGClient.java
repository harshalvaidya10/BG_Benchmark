/**
 * @description
 * @date 2025/3/27 10:53
 * @version 1.0
 */

package janusgraph;
/**
 * DummyBGClient:
 *   1) 从命令行读取:
 *        args[0] = minThreadCount (int)
 *        args[1] = maxThreadCount (int)
 *   2) 使用二次函数 performance = -(threadCount - 50)^2 + 2500
 *      来模拟在 threadCount 范围内的吞吐量。
 *   3) 用 while 循环遍历 [minThreadCount, maxThreadCount]，找出能
 *      带来最大 performance 的线程数 bestT。
 *   4) 在控制台输出最佳线程数和其对应的吞吐量。
 *
 * 用法示例：
 *   java DummyBGClient 1 100
 *
 * 输出：
 *   找到的最大吞吐量对应线程数，以及性能数值。
 */
/**
 * A DummyBGClient that:
 *   1) Reads command-line arguments:
 *        args[0] = minThreadCount (int)
 *        args[1] = maxThreadCount (int)
 *   2) Uses a binary search on the integer range [minThreadCount, maxThreadCount]
 *      to find the threadCount (T) that gives the maximum performance:
 *        performance = -(T - 50)^2 + 2500
 *   3) Prints the best T and its corresponding performance.
 *
 * Usage Example:
 *   java DummyBGClient 1 100
 */
public class DummyBGClient {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java DummyBGClient <minThreadCount> <maxThreadCount>");
            System.exit(1);
        }

        int minThreadCount;
        int maxThreadCount;

        try {
            minThreadCount = Integer.parseInt(args[0]);
            maxThreadCount = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid arguments. Please provide integer values for thread counts.");
            System.exit(1);
            return;
        }

        if (minThreadCount > maxThreadCount) {
            System.err.println("minThreadCount must be <= maxThreadCount.");
            System.exit(1);
        }

        int bestT = findPeakThreadCount(minThreadCount, maxThreadCount);
        double bestPerf = computeQuadraticPerformance(bestT);

        System.out.println("===========================================");
        System.out.println("Best ThreadCount (bestT)     = " + bestT);
        System.out.println("Corresponding Performance    = " + bestPerf);
        System.out.println("===========================================");
    }

    /**
     * 二分搜索，用于在 [left, right] 范围内，基于二次函数（单峰）特性，
     * 寻找使 performance 最大的线程数。
     *
     * 逻辑（针对单峰函数的离散场景）：
     *   - 循环，直到 left >= right 为止
     *   - 令 mid = (left + right) / 2
     *   - 比较 performance(mid) 与 performance(mid + 1)
     *     * 若 performance(mid) < performance(mid + 1)，说明峰值在 mid+1 右侧
     *       -> left = mid + 1
     *     * 否则峰值在 mid（或左侧），right = mid
     *   - 当 left == right 时即找到峰值位置
     */
    private static int findPeakThreadCount(int left, int right) {
        while (left < right) {
            int mid = (left + right) / 2;

            double perfMid = computeQuadraticPerformance(mid);
            double perfMidPlus = computeQuadraticPerformance(mid + 1);

            if (perfMid < perfMidPlus) {
                // 峰值在 mid+1 或更右边
                left = mid + 1;
            } else {
                // 峰值在 mid 或左边
                right = mid;
            }
        }
        // 此时 left == right，为峰值位置
        return left;
    }

    /**
     * 二次函数性能模型：
     *    performance(t) = -(t - 50)^2 + 2500
     * 在 t=50 时达到最大值 2500。
     */
    private static double computeQuadraticPerformance(int threadCount) {
        return -Math.pow(threadCount - 50, 2) + 2500;
    }
}
