package com.taxilla.scheduler.compatibility;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.taxilla.scheduler.DbUtils;
import com.taxilla.scheduler.EmbeddedPostgresqlExtension;

import javax.sql.DataSource;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

    @Override
    public DataSource getDataSource() {
        return postgres.getDataSource();
    }

}
