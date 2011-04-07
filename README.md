node-cassandra-client
====================

node-cassandra-client is an idiomatic [Node.js](http://nodejs.org) client for [Apache Cassandra](http://cassandra.apache.org).
It deals with thrift so you can do other things.

Dependencies
====================

Just thrift >= 0.6.
  $ npm install thrift

Using It
====================

### Connect to a keyspace
    var Keyspace = require('node-cassandra-client').Keyspace;
    var keyspace1 = new Keyspace('Keyspace1', ['hosta:1234', 'hostb:1234', 'hostc:1234']);
    
### Insert some data
    keyspace1.insert('key0', 'Standard1', null, 'cola', 'valuea', 0, function(err) {
      assert.ifError(err);
    });

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