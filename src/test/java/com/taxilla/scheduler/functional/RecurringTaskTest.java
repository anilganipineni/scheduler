package com.taxilla.scheduler.functional;

import com.taxilla.scheduler.DbUtils;
import com.taxilla.scheduler.EmbeddedPostgresqlExtension;
import com.taxilla.scheduler.ScheduledExecution;
import com.taxilla.scheduler.TestTasks;
import com.taxilla.scheduler.task.TaskInstanceId;
import com.taxilla.scheduler.task.helper.RecurringTask;
import com.taxilla.scheduler.task.helper.Tasks;
import com.taxilla.scheduler.task.schedule.Schedules;
import com.taxilla.scheduler.testhelper.ManualScheduler;
import com.taxilla.scheduler.testhelper.SettableClock;
import com.taxilla.scheduler.testhelper.TestHelper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import static co.unruly.matchers.OptionalMatchers.contains;
import static org.hamcrest.MatcherAssert.assertThat;

public class RecurringTaskTest {

    public static final ZoneId ZONE = ZoneId.systemDefault();
    private static final LocalDate DATE = LocalDate.of(2018, 3, 1);
    private static final LocalTime TIME = LocalTime.of(8, 0);
    private SettableClock clock;

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();


    @BeforeEach
    public void setUp() {
        clock = new SettableClock();
        clock.set(ZonedDateTime.of(DATE, TIME, ZONE).toInstant());
    }

    @Test
    public void should_have_starttime_according_to_schedule_by_default() {

        RecurringTask<Void> recurringTask = Tasks.recurring("recurring-a", Schedules.daily(LocalTime.of(23, 59)))
                .execute(TestTasks.DO_NOTHING);

        ManualScheduler scheduler = TestHelper.createManualScheduler(postgres.getDataSource())
                .clock(clock)
                .startTasks(Arrays.asList(recurringTask))
                .build();

        scheduler.start();

        Optional<ScheduledExecution<Object>> firstExecution = scheduler.getScheduledExecution(TaskInstanceId.of("recurring-a", RecurringTask.INSTANCE));
        assertThat(firstExecution.map(ScheduledExecution::getExecutionTime),
                contains(ZonedDateTime.of(DATE, LocalTime.of(23, 59), ZONE).toInstant()));
    }

    @Test
    public void should_have_starttime_now_if_overridden_by_schedule() {

        RecurringTask<Void> recurringTask = Tasks.recurring("recurring-a", Schedules.fixedDelay(Duration.ofHours(1)))
                .execute(TestTasks.DO_NOTHING);

        ManualScheduler scheduler = TestHelper.createManualScheduler(postgres.getDataSource())
                .clock(clock)
                .startTasks(Arrays.asList(recurringTask))
                .build();
        scheduler.start();

        Optional<ScheduledExecution<Object>> firstExecution = scheduler.getScheduledExecution(TaskInstanceId.of("recurring-a", RecurringTask.INSTANCE));

        assertThat(firstExecution.map(ScheduledExecution::getExecutionTime),
                contains(ZonedDateTime.of(DATE, TIME, ZONE).toInstant()));
    }

}
