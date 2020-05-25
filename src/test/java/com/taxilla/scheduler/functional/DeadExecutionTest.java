package com.taxilla.scheduler.functional;

import com.taxilla.scheduler.DbUtils;
import com.taxilla.scheduler.EmbeddedPostgresqlExtension;
import com.taxilla.scheduler.Scheduler;
import com.taxilla.scheduler.StopSchedulerExtension;
import com.taxilla.scheduler.helper.TestableRegistry;
import com.taxilla.scheduler.task.CompletionHandler;
import com.taxilla.scheduler.task.ExecutionComplete;
import com.taxilla.scheduler.task.ExecutionOperations;
import com.taxilla.scheduler.task.helper.CustomTask;
import com.taxilla.scheduler.task.helper.Tasks;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;

import static com.taxilla.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeadExecutionTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @Test
    public void test_dead_execution() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            CustomTask<Void> customTask = Tasks.custom("custom-a", Void.class)
                .execute((taskInstance, executionContext) -> new CompletionHandler<Void>() {
                    @Override
                    public void complete(ExecutionComplete executionComplete, ExecutionOperations<Void> executionOperations) {
                        //do nothing on complete, row will be left as-is in database
                    }
                });

            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(2);

            TestableRegistry registry = TestableRegistry.create().waitConditions(completedCondition).build();

            Scheduler scheduler = Scheduler.create(postgres.getDataSource(), customTask)
                .pollingInterval(Duration.ofMillis(100))
                .heartbeatInterval(Duration.ofMillis(100))
                .statsRegistry(registry)
                .build();
            stopScheduler.register(scheduler);

            scheduler.schedule(customTask.instance("1"), Instant.now());
            scheduler.start();
            completedCondition.waitFor();

            assertEquals(registry.getCount(SchedulerStatsEvent.DEAD_EXECUTION), 1);

        });
    }

}
