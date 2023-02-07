package org.apache.seatunnel.connectors.doris.util;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;

/**
 * 错误重试工具类
 *
 * @author y
 * @date
 */
@Slf4j
public abstract class TryUtil {
    /**
     * 重试调度方法
     *
     * @param dataFunction     返回数据方法执行体
     * @param exceptionCaught  出错异常处理(包括第一次执行和重试错误)
     * @param retryCount       重试次数
     * @param sleepTime        重试间隔睡眠时间(注意：阻塞当前线程)
     * @param expectExceptions 期待异常(抛出符合相应异常时候重试),空或者空容器默认进行重试
     * @param <R>              数据类型
     * @return R
     */
    public static <R> R invoke(Function<R> dataFunction, Consumer<Throwable> exceptionCaught, int retryCount, long sleepTime, List<Class<? extends Throwable>> expectExceptions) {
        Throwable ex;
        try {
            // 产生数据
            return dataFunction == null ? null : dataFunction.run(-1);
        } catch (Throwable throwable) {
            // 捕获异常
            catchException(exceptionCaught, throwable);
            ex = throwable;
        }

        if (expectExceptions != null && !expectExceptions.isEmpty()) {
            // 校验异常是否匹配期待异常
            Class<? extends Throwable> exClass = ex.getClass();
            boolean match = expectExceptions.stream().anyMatch(clazz -> clazz == exClass);
            if (!match) {
                return null;
            }
        }

        // 匹配期待异常或者允许任何异常重试
        for (int i = 0; i < retryCount; i++) {
            try {
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
                return dataFunction.run(i);
            } catch (InterruptedException e) {
                System.err.println("thread interrupted !! break retry,cause:" + e.getMessage());
                // 恢复中断信号
                Thread.currentThread().interrupt();
                // 线程中断直接退出重试
                break;
            } catch (Throwable throwable) {
                catchException(exceptionCaught, throwable);
            }
        }

        return null;
    }

    private static void catchException(Consumer<Throwable> exceptionCaught, Throwable throwable) {
        try {
            if (exceptionCaught != null) {
                exceptionCaught.accept(throwable);
            }
        } catch (Throwable e) {
            log.error("retry exception caught throw error:{}", e.getMessage());
        }
    }

    @FunctionalInterface
    public interface Function<T> {

        /**
         * Gets a result.
         *
         * @return a result
         * @throws Exception 错误时候抛出异常
         */
        T run(int i) throws Exception;
    }

}