package com.github.anilganipineni.scheduler.compatibility;

import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.anilganipineni.scheduler.EmbeddedPostgresqlExtension;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @Override
    public SchedulerDataSource getDataSource() {
        return postgres;
    }

}
