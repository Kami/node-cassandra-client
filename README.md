knode-cassandra-client
====================

node-cassandra-client is an idiomatic [Node.js](http://nodejs.org) client for [Apache Cassandra](http://cassandra.apache.org).
It deals with thrift so you can do other things.

Dependencies
====================

thrift and logmagic

  $ npm install thrift
  $ npm install logmagic

Using It
====================

### Access the System keyspace
    var System = require('node-cassandra-client').System;
    var sys = new System('127.0.0.1:9160');
    
    sys.describeKeyspace('Keyspace1', function(err, ksDef) {
      if (err) {
        // this code path is executed if the key space does not exist.
      } else {
        // assume ksDef contains a full description of the keyspace (uses the thrift structure).
      }
    }
    
### Create a keyspace
    sys.addKeyspace(ksDef, function(err) {
      if (err) {
        // there was a problem creating the keyspace.
      } else {
        // keyspace was successfully created.
      }
    });
    
### Updating
This example assumes you have strings for keys, column names and values:
    var Connection = require('node-cassandra-client').Connection;
    var con = new Connection('user', 'password', 'cassandra-host', 9160, 'Keyspace1');
    con.execute('UPDATE Standard1 SET ?=? WHERE key=?', ['cola', 'valuea', 'key0'], function(err) {
        if (err) {
            // handle error
        } else {
            // handle success.
        }
	});

### Getting data (single row result)
    con.execute('SELECT ? FROM Standard1 WHERE key=?', ['cola', 'key0'], function(err, row) {
        if (err) {
            // handle error
        } else {
            assert.ok(row.colHash['cola']);
            assert.ok(row.cols[0].name === 'cola');
            assert.ok(row.cols[0].value === 'valuea');
        }
    });

### Getting data (multiple rows)
Assume the updates have happened previously.  todo: figure out; I can't get this part working consistenently.
	
Things you should know about
============================
### Result Ordering
Right now, if your select clause selects individual columns (not a slice), those columns will come back
in comparator order and not the order specified in your select clause (see [CASSANDRA-2493](https://issues.apache.org/jira/browse/CASSANDRA-2493)).

### Numbers
The Javascript Number type doesn't match up well with the java longs and integers stored in Cassandra.
Therefore all numbers returned in queries are BigIntegers.  This means that you need to be careful when you
do updates.  If you're worried about losing precision, specify your numbers as strings.