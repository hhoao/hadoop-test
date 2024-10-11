CREATE TABLE %s (
  id INT,
  name STRING,
  age INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = '%s',
  'index' = 'users'
);
