package com.github.anilganipineni.scheduler.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.StopSchedulerExtension;
import com.github.anilganipineni.scheduler.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.anilganipineni.scheduler.task.CompletionHandler;
import com.github.anilganipineni.scheduler.task.ExecutionComplete;
import com.github.anilganipineni.scheduler.task.ExecutionOperations;
import com.github.anilganipineni.scheduler.task.helper.CustomTask;
import com.github.anilganipineni.scheduler.task.helper.Tasks;

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

            Scheduler scheduler = Scheduler.create(postgres, customTask)
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
