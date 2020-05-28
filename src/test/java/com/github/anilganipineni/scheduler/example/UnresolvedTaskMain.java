package com.github.anilganipineni.scheduler.example;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.HsqlTestDatabaseExtension;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerClient;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.task.helper.RecurringTask;
import com.github.anilganipineni.scheduler.task.helper.Tasks;
import com.github.anilganipineni.scheduler.task.schedule.Schedules;

public class UnresolvedTaskMain {
    private static final Logger LOG = LoggerFactory.getLogger(UnresolvedTaskMain.class);

    private static void example(SchedulerDataSource dataSource) {

        RecurringTask<Void> unresolvedTask = Tasks.recurring("unresolved1", Schedules.fixedDelay(Duration.ofSeconds(1)))
                .execute((taskInstance, executionContext) -> {
                    System.out.println("Ran");
                });
        RecurringTask<Void> unresolvedTask2 = Tasks.recurring("unresolved2", Schedules.fixedDelay(Duration.ofSeconds(1)))
            .execute((taskInstance, executionContext) -> {
                System.out.println("Ran");
            });

        SchedulerClient client = SchedulerClient.Builder.create(dataSource).build();
        client.schedule(unresolvedTask.instance(RecurringTask.INSTANCE), Instant.now());
        client.schedule(unresolvedTask2.instance(RecurringTask.INSTANCE), Instant.now().plusSeconds(10));

        final Scheduler scheduler = Scheduler
                .create(dataSource)
                .pollingInterval(Duration.ofSeconds(1))
                .heartbeatInterval(Duration.ofSeconds(5))
                .deleteUnresolvedAfter(Duration.ofSeconds(20))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));

        scheduler.start();

        IntStream.range(0, 5).forEach(i -> {
            scheduler.getScheduledExecutions(e -> {});
            scheduler.getFailingExecutions(Duration.ZERO);
        });
    }

    public static void main(String[] args) throws Throwable {
        try {
            final HsqlTestDatabaseExtension hsqlRule = new HsqlTestDatabaseExtension();
            hsqlRule.beforeEach(null);
            example(hsqlRule);
        } catch (Exception e) {
            LOG.error("Error", e);
        }

    }

}