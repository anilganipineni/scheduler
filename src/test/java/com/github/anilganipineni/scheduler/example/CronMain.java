package com.github.anilganipineni.scheduler.example;

import com.github.anilganipineni.scheduler.HsqlTestDatabaseExtension;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.task.helper.RecurringTask;
import com.github.anilganipineni.scheduler.task.helper.Tasks;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;
import com.github.anilganipineni.scheduler.task.schedule.Schedules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

public class CronMain {
    private static final Logger LOG = LoggerFactory.getLogger(CronMain.class);

    private static void example(DataSource dataSource) {

        Schedule cron = Schedules.cron("*/10 * * * * ?");
        RecurringTask<Void> cronTask = Tasks.recurring("cron-task", cron)
                .execute((taskInstance, executionContext) -> {
                    System.out.println(Instant.now().getEpochSecond() + "s  -  Cron-schedule!");
                });

        final Scheduler scheduler = Scheduler
                .create(dataSource)
                .startTasks(cronTask)
                .pollingInterval(Duration.ofSeconds(1))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));

        scheduler.start();
    }

    public static void main(String[] args) throws Throwable {
        try {
            final HsqlTestDatabaseExtension hsqlRule = new HsqlTestDatabaseExtension();
            hsqlRule.beforeEach(null);

            final DataSource dataSource = hsqlRule.getDataSource();

            example(dataSource);
        } catch (Exception e) {
            LOG.error("Error", e);
        }

    }

}
