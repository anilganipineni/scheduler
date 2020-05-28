package com.github.anilganipineni.scheduler.testhelper;

import java.util.Date;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.task.helper.RecurringTask;
import com.github.anilganipineni.scheduler.task.helper.Tasks;
import com.github.anilganipineni.scheduler.task.schedule.FixedDelay;

/**
 * @author akganipineni
 */
public class Tester {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		startScheduler(new DataSourceScheduler());
	}
	/**
	 * 
	 */
	private static void startScheduler(SchedulerDataSource dataSource) {
		RecurringTask<Void> hourlyTask = Tasks.recurring("my-hourly-task", FixedDelay.ofHours(1))
		        .execute((inst, ctx) -> {
		        	System.out.println(new Date() + " - Anil Scheduler Executed..........");
		        });
		RecurringTask<Void> minutesTask = Tasks.recurring("my-minutes-task", FixedDelay.ofMinutes(5))
		        .execute((inst, ctx) -> {
		        	System.out.println(new Date() + " - Anil minutes Scheduler Executed..........");
		        });
		System.out.println(new Date() + " - Enabling the Anil Scheduler..........");
		Scheduler s = Scheduler.create(dataSource).startTasks(hourlyTask, minutesTask).threads(5).build();

		// Task(s) will automatically scheduled on startup if not already started (i.e. if not exists in the db)
		s.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    @Override
		    public void run() {
				System.out.println(new Date() + " - Received shutdown signal.");
				if(s != null) {
					s.stop();
				}
		    }
		});
	}
}
