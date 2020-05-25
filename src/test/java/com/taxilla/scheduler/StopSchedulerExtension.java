package com.taxilla.scheduler;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.taxilla.scheduler.Scheduler;

import java.util.stream.Stream;

public class StopSchedulerExtension implements AfterEachCallback {

    private Scheduler[] scheduler = new Scheduler[]{};

    public void register(Scheduler ... scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Stream.of(scheduler).forEach(Scheduler::stop);
    }
}
