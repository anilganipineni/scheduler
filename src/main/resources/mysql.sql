CREATE TABLE scheduled_tasks (
  task_name varchar(40) NOT NULL,
  task_id varchar(40) NOT NULL,
  consecutive_failures int(11) DEFAULT NULL,
  execution_time timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  last_failure timestamp(6) NULL DEFAULT NULL,
  last_heartbeat timestamp(6) NULL DEFAULT NULL,
  last_success timestamp(6) NULL DEFAULT NULL,
  picked tinyint(1) NOT NULL,
  picked_by varchar(50) DEFAULT NULL,
  task_data varchar(2000) DEFAULT NULL,
  version int(6) NOT NULL,
  PRIMARY KEY (task_name,task_id)
)