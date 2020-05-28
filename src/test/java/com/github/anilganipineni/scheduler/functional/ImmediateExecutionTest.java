package com.github.anilganipineni.scheduler.functional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.StopSchedulerExtension;
import com.github.anilganipineni.scheduler.TestTasks;
import com.github.anilganipineni.scheduler.helper.TestableRegistry;
import com.github.anilganipineni.scheduler.task.ExecutionComplete;
import com.github.anilganipineni.scheduler.task.helper.OneTimeTask;
import com.github.anilganipineni.scheduler.testhelper.SettableClock;

import co.unruly.matchers.TimeMatchers;


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

            Scheduler scheduler = Scheduler.create(postgres, task)
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
