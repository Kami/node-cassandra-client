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
    
### Connect to a column family
    var ColumnFamily = require('node-cassandra-client').ColunmFamily;
    var cfWithLongs = new ColumnFamily('Keyspace1', 'CfLong', null, null, '127.0.0.1', 9160);
    cfWithLongs.insert(1, {1:2, 3:4, 5:6, 7:8}, 0, 'ONE', function(err) {
        if (err) {
            // there was a problem with the update.
        } else {
            // alles ist gut.
        }
	});

Maybe your column family uses UUIDs everwhere:

	var cfWithUUID = new ColumnFamily('Keyspace1', 'CfUuid', null, null, '127.0.0.1', 9160);
	cfWithUUID.insert('6f8483b0-65e0-11e0-0000-fe8ebeead9fe',
	                  {'6f8483b0-65e0-11e0-0000-fe8ebeead9fe': '6fd45160-65e0-11e0-0000-fe8ebeead9fe',
	                   '6fd589e0-65e0-11e0-0000-7fd66bb03aff': '6fd6e970-65e0-11e0-0000-fe8ebeead9fe'},
	                  0, 
	                  'ONE', 
	                  function(err) {
	                    CfUUID.close();
	                    if (err) {
	                      // there was a problem.
	                    }
	                  });

### TODO: select