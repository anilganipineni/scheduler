package com.github.anilganipineni.scheduler.testhelper;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerBuilder;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.schedule.FixedDelay;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.TaskFactory;

/**
 * @author akganipineni
 */
public class Tester {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		startScheduler(new SchedulerDataSourceImpl());
	}
	/**
	 * 
	 */
	private static void startScheduler(SchedulerDataSource dataSource) {
		Task hourlyTask = TaskFactory.recurring("my-hourly-task", FixedDelay.ofHours(1))
		        .execute((inst, ctx) -> {
		        	System.out.println(new Date() + " - Anil Scheduler Executed..........");
		        });
		Task minutesTask = TaskFactory.recurring("my-minutes-task", FixedDelay.ofMinutes(5))
		        .execute((inst, ctx) -> {
		        	System.out.println(new Date() + " - Anil minutes Scheduler Executed..........");
		        });
		System.out.println(new Date() + " - Enabling the Anil Scheduler..........");
		
		List<Task> startTasks = Arrays.asList(hourlyTask, minutesTask);
		Scheduler s = SchedulerBuilder.create(dataSource).threads(5).enableImmediateExecution().startTasks(startTasks).build();

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
		
		Runnable r = new Runnable() {
			/**
			 * @see java.lang.Runnable#run()
			 */
			@Override
			public void run() {
				try {
					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Line 65 ");
					Thread.sleep(1 * 60 * 1000);
					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Line 67 ");
					OneTimeTask myAdhocTask = TaskFactory.oneTime("my-typed-adhoc-task")
			                .execute((inst, ctx) -> {
			                	System.out.println(new Date() + " - " + Thread.currentThread().getName() + "Executed! Custom data, Id: " + inst.getId());
			                });

					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Line 73 ");
					Map<String, Object> map = new HashMap<String, Object>();
					map.put("id", Long.parseLong("1001"));

					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Line 77 ");
 					s.schedule(myAdhocTask.instance("1045", map), Instant.now().plusSeconds(5));
					System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Thread executed........... ");
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		};
		new Thread(r, "New Task Thread ").start();
	}
}
