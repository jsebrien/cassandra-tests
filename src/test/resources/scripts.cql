CREATE KEYSPACE pizzastore WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
use pizzastore;

CREATE TABLE pizzas (
  pizza_id text PRIMARY KEY,
  name text,
  calories decimal,
  ingredients set<text>,
  recipies_sites list<text>,
  molecules map<text, double>
);