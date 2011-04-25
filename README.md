node-cassandra-client
====================

node-cassandra-client is a [Node.js](http://nodejs.org) CQL driver for [Apache Cassandra](http://cassandra.apache.org) 0.8 and later.

CQL is a query language for Apache Cassandra.  You use it in much the same way you would use SQL for a relational database.
The Cassandra [documentation](https://svn.apache.org/viewvc/cassandra/trunk/doc/cql/CQL.html?view=co) can help you learn the syntax.

License
====================

node-cassandra-client is distributed under the [Apache license](http://www.apache.org/licenses/LICENSE-2.0.html).

[lib/bigint.js](https://github.com/racker/node-cassandra-client/blob/master/lib/bigint.js) is [borrowed](https://github.com/joyent/node/blob/master/deps/v8/benchmarks/crypto.js)
from the Node.js source (which comes from the [V8](http://code.google.com/p/v8/) source).

Installation
====================

    $ npm install cassandra-client

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
	
### Pooled Connections
    // Creating a new connection pool.
    var PooledConnection = require('node-cassandra-client').PooledConnection;
    var hosts = ['host1:9160', 'host2:9170', 'host3', 'host4'];
    var connection_pool = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1'});

PooledConnection() accepts an objects with these slots:

         hosts : String list in host:port format. Port is optional (defaults to 9160).
      keyspace : Name of keyspace to use.
          user : User for authentication (optional).
          pass : Password for authentication (optional).
       maxSize : Maximum number of connection to pool (optional).
    idleMillis : Idle connection timeout in milliseconds (optional).

Queries are performed using the `execute()` method in the same manner as `Connection`,
(see above).  For example:

    // Writing
    connection_pool.execute('UPDATE Standard1 SET ?=? WHERE KEY=?', ['A', '1', 'K'],
      function(err) {
        if (err) console.log("failure");
        else console.log("success");
      }
    );
    
    // Reading
    connection_pool.execute('SELECT ? FROM Standard1 WHERE KEY=?', ['A', 'K'],
      function(err, row) {
        if (err) console.log("lookup failed");
        else console.log("got result " + row.cols[0].value);
      }
    );

When you are finished with a `PooledConnection` instance, call `shutdown(callback)`.
Shutting down the pool prevents further work from being enqueued, and closes all 
open connections after pending requests are complete.

    // Shutting down a pool
    connection_pool.shutdown(function() { console.log("connection pool shutdown"); });


Things you should know about
============================
### Numbers
The Javascript Number type doesn't match up well with the java longs and integers stored in Cassandra.
Therefore all numbers returned in queries are BigIntegers.  This means that you need to be careful when you
do updates.  If you're worried about losing precision, specify your numbers as strings and use the BigInteger library.

### Decoding
node-cassandra-client supports Cassandra `BytesType`, `IntegerType`, `LongTime` and `TimeUUIDType` out of the box.  
When dealing with numbers, the values you retreive out of rows will all be `BigInteger`s (be wary of losing precision
if your numbers are bigger than 2^53--you know, like a timestamp).

`BigInteger` supports many operations like add, subtract, multiply, etc., and a few others that may come in handy: shift, square, abs, etc.  Check the source if you'd like to know more.

We technically have a [UUID type](https://github.com/racker/node-cassandra-client/blob/master/lib/uuid.js), but have not had the need to flesh it out yet.  If you find the need to expose more parts of the UUID (timestamp, node, clock sequence, etc.), or would like to implement some operations, patches are welcome.

### Todo
* Full BigInteger documentation.
