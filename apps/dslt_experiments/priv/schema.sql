create table if not exists metric_id (
  metric_name TEXT UNIQUE NOT NULL,
  id SMALLSERIAL PRIMARY KEY
);

insert into metric_id VALUES ('t'), ('worker_start_time'), ('worker_stop_time')
on conflict do nothing;

create table if not exists ds_db (
   db_name TEXT UNIQUE NOT NULL,

   backend TEXT NOT NULL,
   n_shards SMALLINT NOT NULL,
   n_replicas SMALLINT,
   config TEXT,
   id SERIAL PRIMARY KEY
);

create table if not exists experiment (
  scenario TEXT,
  completed_at BIGINT NOT NULL,
  db SMALLINT references ds_db(id),
  repeats INTEGER,
  n_workers INTEGER,
  payload_size INTEGER,
  id SERIAL PRIMARY KEY
);

create table if not exists sample (
  metric SMALLINT references metric_id(id) NOT NULL,
  experiment INTEGER references experiment(id) NOT NULL,
  worker_id INTEGER,
  val BIGINT NOT NULL
);
