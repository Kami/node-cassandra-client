node-cassandra-client
====================

node-cassandra-client is an idiomatic [Node.js](http://nodejs.org) client for [Apache Cassandra](http://cassandra.apache.org).
It deals with thrift so you can do other things.

License
====================

node-cassandra-client is distributed under the [Apache license](http://www.apache.org/licenses/LICENSE-2.0.html).

[lib/bigint.js](lib/bigint.js) is [borrowed](https://github.com/joyent/node/blob/master/deps/v8/benchmarks/crypto.js)
from the Node.js source (which comes from the [V8](http://code.google.com/p/v8/) source).

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
    var con = new Connection('cassandra-host', 9160, 'Keyspace1', 'user', 'password');
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
**NOTE:** You'll only get ordered and meaningful results if you are using an order-preserving partitioner.
Assume the updates have happened previously.

	con.execute('SELECT ? FROM Standard1 WHERE key >= ? and key <= ?', ['cola', 'key0', 'key1'], function (err, rows) {
		if (err) {
			// handle error
		} else {
			console.log(rows.rowCount());
			console.log(rows[0]); // behaves just like row in the above example.
		}
	});
	
Things you should know about
============================
### Result Ordering
Right now, if your select clause selects individual columns (not a slice), those columns will come back
in comparator order and not the order specified in your select clause (see [CASSANDRA-2493](https://issues.apache.org/jira/browse/CASSANDRA-2493)).

### Numbers
The Javascript Number type doesn't match up well with the java longs and integers stored in Cassandra.
Therefore all numbers returned in queries are BigIntegers.  This means that you need to be careful when you
do updates.  If you're worried about losing precision, specify your numbers as strings and use the BigInteger library.

### TODO
* connection pool support
* document decoding
