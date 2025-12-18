create extension if not exists timescaledb;

create table if not exists metric_id (
  metric_name TEXT UNIQUE NOT NULL,
  id SMALLSERIAL PRIMARY KEY
);

insert into metric_id VALUES ('t'), ('worker_start_time'), ('worker_stop_time')
on conflict do nothing;

create table if not exists ds_db (
   db_name TEXT NOT NULL,
   backend TEXT NOT NULL,
   n_shards SMALLINT NOT NULL,
   n_replicas SMALLINT,
   config TEXT NOT NULL,
   id SERIAL PRIMARY KEY
);

create table if not exists experiment (
  scenario TEXT,
  release TEXT,
  completed_at BIGINT,
  db SMALLINT references ds_db(id),
  repeats INTEGER,
  n_workers INTEGER,
  payload_size INTEGER,
  batch_size INTEGER,
  id SERIAL PRIMARY KEY
);

create table if not exists sample (
  metric SMALLINT references metric_id(id) NOT NULL,
  t timestamp NOT NULL,
  experiment INTEGER references experiment(id) NOT NULL,
  worker_id INTEGER,
  val BIGINT NOT NULL
) with (
  timescaledb.hypertable,
  timescaledb.partition_column = t,
  timescaledb.chunk_interval = '1 min'
);

create or replace view samples as
 select metric_name as metric, t, experiment, worker_id, val from sample inner join metric_id on metric_id.id = sample.metric;

create or replace function db_id(n TEXT, b TEXT, s SMALLINT, nr SMALLINT, conf TEXT)
returns integer
as $$
declare
  r INTEGER;
begin
  r := (SELECT id AS i FROM ds_db WHERE db_name = n AND backend = b AND n_shards = s AND n_replicas = nr AND config = conf);
  if r IS NULL THEN
     INSERT INTO ds_db VALUES (n, b, s, nr, conf) RETURNING id INTO r;
  end if;
  return r;
end;
$$ language plpgsql;

create or replace function new_experiment(db_name TEXT, db_backend TEXT, n_shards SMALLINT, n_replicas SMALLINT, db_conf TEXT, scenario TEXT, release TEXT, repeats INTEGER, n_workers INTEGER, payload_size INTEGER, batch_size INTEGER)
returns INTEGER
as $$
declare
  dbid INTEGER;
  ret INTEGER;
begin
  dbid := db_id(db_name, db_backend, n_shards, n_replicas, db_conf);
  INSERT INTO experiment VALUES (scenario, release, NULL, dbid, repeats, n_workers, payload_size, batch_size) RETURNING id INTO ret;
  RETURN ret;
end;
$$ language plpgsql;

create or replace procedure complete_experiment(exp INTEGER, t BIGINT)
as $$
begin
  UPDATE experiment SET completed_at = t WHERE id = exp;
end;
$$ LANGUAGE plpgsql;

create or replace function metric_id(txt TEXT)
returns INTEGER
as $$
declare
  x INTEGER;
begin
  x := (SELECT id FROM metric_id WHERE metric_name = txt);
  if x IS NULL THEN
    INSERT INTO metric_id VALUES (txt) RETURNING id INTO x;
  end if;
  return x;
end;
$$ language plpgsql;

create or replace function compute_tps(exp INTEGER)
returns FLOAT
as $$
declare
  dt INTEGER;
  n INTEGER;
begin
  dt := (select max(val) from samples where metric = 'w_stop_time' and experiment = exp) -
        (select min(val) from samples where metric = 'w_start_time' and experiment = exp);
  n := (select count(1) from samples where metric = 't' and experiment = exp);
  return (n * 1e9) / dt;
end;
$$ language plpgsql;
