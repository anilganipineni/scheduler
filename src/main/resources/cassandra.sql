CREATE TABLE scheduled_tasks (
    task_name text,
    task_id text,
    consecutive_failures int,
    execution_time timestamp,
    last_failure timestamp,
    last_heartbeat timestamp,
    last_success timestamp,
    picked boolean,
    picked_by text,
    task_data text,
    version int,
    PRIMARY KEY (task_name, task_id)
) WITH read_repair_chance = 0.0
   AND dclocal_read_repair_chance = 0.1
   AND gc_grace_seconds = 864000
   AND bloom_filter_fp_chance = 0.01
   AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
   AND comment = ''
   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
   AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
   AND default_time_to_live = 0
   AND speculative_retry = '99PERCENTILE'
   AND min_index_interval = 128
   AND max_index_interval = 2048
   AND crc_check_chance = 1.0;
