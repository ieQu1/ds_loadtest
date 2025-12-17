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

CREATE OR REPLACE FUNCTION db_id(n TEXT, b TEXT, s SMALLINT, nr SMALLINT, conf TEXT)
RETURNS INTEGER
AS $$
DECLARE
  r INTEGER;
BEGIN
  r := (SELECT id AS i FROM ds_db WHERE db_name = n AND backend = b AND n_shards = s AND n_replicas = nr AND config = conf);
  IF r IS NULL THEN
     INSERT INTO ds_db VALUES (n, b, s, nr, conf);
     r := (SELECT id AS i FROM ds_db WHERE db_name = n AND backend = b AND n_shards = s AND n_replicas = nr AND config = conf);
  END IF;
  RETURN r;
END;
$$ LANGUAGE plpgsql;
