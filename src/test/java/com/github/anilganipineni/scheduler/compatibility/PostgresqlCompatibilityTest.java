package com.github.anilganipineni.scheduler.compatibility;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.DbUtils;
import com.github.anilganipineni.scheduler.EmbeddedPostgresqlExtension;

import javax.sql.DataSource;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @Override
    public DataSource getDataSource() {
        return postgres.getDataSource();
    }

}
