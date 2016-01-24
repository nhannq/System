A Java program to:
- Calculate tokens for the Murmur3Partitioner of Cassandra
- Find a token range that a key belongs to

To run the program you need to turn on Apache Cassandra and run the following commands from bin/cassandra-cli:

CREATE KEYSPACE mykeyspace
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE mykeyspace;

CREATE TABLE t2 (
  k int PRIMARY KEY,
  other int
);

This program uses the DataStax Java Driver for Apache Cassandra: https://github.com/datastax/java-driver

