cassandra-tests
===============

Cassandra use cases
====================
This repo contains unit tests, describing Cassandra real life use cases, using only the Datastax Java API.

Installation prerequisites
-------

To run these tests, you need:
- JDK 6 or greater
- Git
- Maven
- Cassandra 1.2 or greater (make sure to have property start_native_transport: true in cassandra.yaml)
- Python 2.7 or greater

Download the project
-------

git clone https://github.com/jsebrien/cassandra-tests.git

Compile the project
-------

mvn package

Start Cassandra
-------
cassandra.bat or ./cassandra

Run cql script
-------
Start cqlsh
- windows : python cqlsh
- linux : cqlsh

and execute the following:

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

Run tests
-------

mvn test

Use cases
-------

- Store collections and maps in column families in cql3 using the Datastax Java Api.
- Bulk insert using bound statements in cql3 using the Datastax Java Api.
- Increment and decrement counters in cql3 using the Datastax Java Api.
