package com.taxilla.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.taxilla.scheduler.JdbcTaskRepository;
import com.taxilla.scheduler.SchedulerName;
import com.taxilla.scheduler.TaskResolver;
import com.taxilla.scheduler.jdbc.JdbcRunner;
import com.taxilla.scheduler.jdbc.RowMapper;
import com.taxilla.scheduler.stats.StatsRegistry;
import com.taxilla.scheduler.task.Execution;
import com.taxilla.scheduler.task.Task;
import com.taxilla.scheduler.task.TaskInstance;
import com.taxilla.scheduler.task.helper.OneTimeTask;

import static com.taxilla.scheduler.jdbc.PreparedStatementSetter.NOOP;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CustomTableNameTest {

    private static final String SCHEDULER_NAME = "scheduler1";

    private static final String CUSTOM_TABLENAME = "custom_tablename";

    @RegisterExtension
    public EmbeddedPostgresqlExtension DB = new EmbeddedPostgresqlExtension();

    private JdbcTaskRepository taskRepository;
    private OneTimeTask<Void> oneTimeTask;

    @BeforeEach
    public void setUp() {
        oneTimeTask = TestTasks.oneTime("OneTime", Void.class, TestTasks.DO_NOTHING);
        List<Task<?>> knownTasks = new ArrayList<>();
        knownTasks.add(oneTimeTask);
        taskRepository = new JdbcTaskRepository(DB.getDataSource(), CUSTOM_TABLENAME, new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerName.Fixed(SCHEDULER_NAME));

        DbUtils.runSqlResource("postgresql_custom_tablename.sql").accept(DB.getDataSource());
    }

    @Test
    public void can_customize_table_name() {
        Instant now = Instant.now();
        TaskInstance<Void> instance1 = oneTimeTask.instance("id1");

        taskRepository.createIfNotExists(new Execution(now, instance1));

        JdbcRunner jdbcRunner = new JdbcRunner(DB.getDataSource());
        jdbcRunner.query("SELECT count(1) AS number_of_tasks FROM " + CUSTOM_TABLENAME, NOOP, (RowMapper<Integer>) rs -> rs.getInt("number_of_tasks"));

    }

    @AfterEach
    public void tearDown() {
        new JdbcRunner(DB.getDataSource()).execute("DROP TABLE " + CUSTOM_TABLENAME, NOOP);
    }

}
