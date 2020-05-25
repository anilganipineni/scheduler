package com.taxilla.scheduler;

import com.google.common.io.CharStreams;
import com.taxilla.scheduler.jdbc.JdbcRunner;
import com.taxilla.scheduler.jdbc.Mappers;
import com.taxilla.scheduler.jdbc.PreparedStatementSetter;

import javax.sql.DataSource;

import static com.taxilla.scheduler.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static com.taxilla.scheduler.jdbc.PreparedStatementSetter.NOOP;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class DbUtils {

    public static void clearTables(DataSource dataSource) {
        new JdbcRunner(dataSource).execute("delete from " + DEFAULT_TABLE_NAME, NOOP);
    }

    public static Consumer<DataSource> runSqlResource(String resource) {
        return dataSource -> {

            final JdbcRunner jdbcRunner = new JdbcRunner(dataSource);
            try {
                final String statements = CharStreams.toString(new InputStreamReader(DbUtils.class.getResourceAsStream(resource)));
                jdbcRunner.execute(statements, NOOP);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static int countExecutions(DataSource dataSource) {
        return new JdbcRunner(dataSource).query("select count(*) from " + DEFAULT_TABLE_NAME,
            PreparedStatementSetter.NOOP, Mappers.SINGLE_INT);
    }
}
