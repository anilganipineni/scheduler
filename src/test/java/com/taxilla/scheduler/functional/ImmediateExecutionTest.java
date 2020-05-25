package com.taxilla.scheduler.functional;

import co.unruly.matchers.TimeMatchers;

import com.taxilla.scheduler.DbUtils;
import com.taxilla.scheduler.EmbeddedPostgresqlExtension;
import com.taxilla.scheduler.Scheduler;
import com.taxilla.scheduler.StopSchedulerExtension;
import com.taxilla.scheduler.TestTasks;
import com.taxilla.scheduler.helper.TestableRegistry;
import com.taxilla.scheduler.task.ExecutionComplete;
import com.taxilla.scheduler.task.helper.OneTimeTask;
import com.taxilla.scheduler.testhelper.SettableClock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;


public class ImmediateExecutionTest {

    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
    @RegisterExtension
    public StopSchedulerExtension stopScheduler = new StopSchedulerExtension();

    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
    }

    @Test
    public void test_immediate_execution() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

            Instant now = Instant.now();
            OneTimeTask<Void> task = TestTasks.oneTime("onetime-a", Void.class, TestTasks.DO_NOTHING);
            TestableRegistry.Condition completedCondition = TestableRegistry.Conditions.completed(1);
            TestableRegistry.Condition executeDueCondition = TestableRegistry.Conditions.ranExecuteDue(1);

            TestableRegistry registry = TestableRegistry.create().waitConditions(executeDueCondition, completedCondition).build();

            Scheduler scheduler = Scheduler.create(postgres.getDataSource(), task)
                .pollingInterval(Duration.ofMinutes(1))
                .enableImmediateExecution()
                .statsRegistry(registry)
                .build();
            stopScheduler.register(scheduler);

            scheduler.start();
            executeDueCondition.waitFor();

            scheduler.schedule(task.instance("1"), clock.now());
            completedCondition.waitFor();

            List<ExecutionComplete> completed = registry.getCompleted();
            assertThat(completed, hasSize(1));
            completed.stream().forEach(e -> {
                assertThat(e.getResult(), is(ExecutionComplete.Result.OK));
                Duration durationUntilExecuted = Duration.between(now, e.getTimeDone());
                assertThat(durationUntilExecuted, TimeMatchers.shorterThan(Duration.ofSeconds(1)));
            });
            registry.assertNoFailures();
        });
    }

}
