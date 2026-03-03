package com.uber.data.kafka.connect.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 * This is a simple wrapper around the standard library and Guava's thread utils that can
 * be used to create and work with executors.
 *
 * @see Executors
 * @see ExecutorService
 * @see ScheduledExecutorService
 * @see ThreadFactory
 * @see ThreadFactoryBuilder
 */
public class ExecutorUtils {
    private static final Logger log = LoggerFactory.getLogger(ExecutorUtils.class);

    private ExecutorUtils() {
    }

    /**
     * Create a new executor that uses the given thread name format and for which
     * all created threads are daemons
     *
     * @param nameFormat the name format for executor threads
     * @return the executor; never null
     */
    public static ScheduledExecutorService singleThreadedScheduled(String nameFormat) {
        return Executors.newSingleThreadScheduledExecutor(threadFactory(nameFormat));
    }

    /**
     * Shut down the executor, waiting for the specified timeout for all running
     * and scheduled tasks to complete
     *
     * @param executor the executor to shut down; may not be null
     * @param executorDescription a description of the executor to use in log messages;
     *                            for example, "offset monitor" or "topic watcher service"
     * @param timeout the time to wait for all tasks to complete
     * @param unit the unit for the timeout
     */
    public static void shutdown(
            ExecutorService executor,
            String executorDescription,
            long timeout,
            TimeUnit unit
    ) {
        executor.shutdown();
        boolean shutdownSucceeded = false;
        boolean interrupted = false;
        try {
            shutdownSucceeded = executor.awaitTermination(timeout, unit);
            if (!shutdownSucceeded) {
                log.warn("Could not shut down {} in time", executorDescription);
            } else {
                log.info("Gracefully shut down {}", executorDescription);
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while shutting down {}", executorDescription, e);
            interrupted = true;
        } finally {
            if (!shutdownSucceeded) {
                executor.shutdownNow();
            }

            // Preserve the interrupt status when control is returned to the caller
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(true)
                .build();
    }
}
