
/** node.js driver for Cassandra-CQL. */

// todo: prepared statements (will need to figure out how to decode typed columns).

var console = require('console');
var sys = require('sys');
var EventEmitter = require('events').EventEmitter;

var thrift = require('thrift');
var Cassandra = require('./gen-nodejs/Cassandra');
var ttypes = require('./gen-nodejs/cassandra_types');

var Decoder = require('./decoder').Decoder;

/** Naïve FIFO queue */
function Queue(maxSize) {
  var items = [];
  var putPtr = 0;
  var takePtr = 0;
  var max = maxSize;
  var curSize = 0;

  this.put = function(obj) {
    if (curSize == max) {
      return false;
    }
    if (items.length < max) {
      items.push(obj);
    }
    else {
      items[putPtr] = obj;
    }
    putPtr = (putPtr + 1) % max;
    curSize += 1;
    return true;
  };

  this.take = function() {
    if (curSize === 0) {
      return false;
    }
    var item = items[takePtr];
    items[takePtr] = null;
    takePtr = (takePtr + 1) % max;
    curSize -= 1;
    return item;
  };

  this.size = function() {
    return curSize;
  };
}

/** 
 * Low level - encapsulates a thrift client and socket 
 **/
DriverThriftConnection = function(host, port) {
  this.isClosing = false;
  this.isConnected = false;
  this.isConnecting = false;
  this.tcon = null;
  this.tclient = null;
  this.isClosed = false;
  
  var self = this;
  
  // the connector is capable of reconnecting the client.
  this.connector = function() {
    // don't continue if connection is pending or already connected.
    if (self.isConnecting) {
      return;
    } else {
      console.log('connecting ' + host + ':' + port );
      self.isConnecting = true;
      var tcon = thrift.createConnection(host, port);
      tcon.on('error', function(err) {
        console.error('ERR_ON_CONNECT ' + host + ':' + port);
        console.error(err);
        self.isClosed = true;
        self.isConnected = false;
        self.isConnecting = false;
        self.tcon = null;
        self.tclient = null;
      });
      tcon.on('close', function() {
        self.isClosed = true;
        self.isConnecting = false;
        self.isConnected = false;
        self.tcon = null;
        self.tclient = null;
        console.log('closed ' + host + ':' + port);
      });
      tcon.on('connect', function() {
        self.isConnecting = false;
        self.isConnected = true;
        console.log('connected ' + host + ':' + port);
      });
      var tclient = thrift.createClient(Cassandra, tcon);
      self.tcon = tcon;
      self.tclient = tclient;
    }
  };
  
  this.close = function() {
    if (this.isClosing) {
      return;
    } else if (this.isConnected) {
      this.isClosing = true;
      this.tcon.end();
    }
  };
};


// used to parse the CF name out of a select statement.
var selectRe = /\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)/im;

/** Statement executes plain text queries */
Statement = module.exports.Statement = function(con) {
  this.con = con;
};

/** update statements. return no results. callback(err) */
Statement.prototype.update = function(cql, callback) {
  this._cql(cql, function(err, res) {
    if (err) {
      callback(err);
    } else {
      if (res.type != ttypes.CqlResultType.VOID) {
        callback('Invalid results for update: ' + res.type);
      } else {
        callback(null);
      }
    }
  });
};

/** query statements.
 * @param cql
 * @param callback(err, res). res is an instances of Results.
 */
Statement.prototype.query = function(cql, callback) {
  var self = this;
  this._cql(cql, function(err, res) {
    if (err) {
      callback(err, null);
    } else if (!res) {
      callback('No results ', null);
    } else {
      var cfName = selectRe.exec(cql)[1];
      var decoder = new Decoder(self.con.validators[cfName]);
      if (res.type != ttypes.CqlResultType.ROWS) {
        callback('Invalid results for update: ' + res.type, null);
      } else {
        // create a Results.
        callback(null, new Results(res, decoder));
      }
    }
  });
};

// performs a generic cql query.
Statement.prototype._cql = function(cql, callback) {
  this.con._putWork(function(c) {
    c.execute_cql_query(cql, ttypes.Compression.NONE, callback);
  });
};




/** this is how query results are display to driver users. */
Results = module.exports.Results = function(res, decoder) {
  this.decoder = decoder;
  // raw rows returned by cassandra cql.
  this.rows = res.rows;
  
  // the current row.
  this.ptr = 0;
  
  // the decoded key.
  this.key = null;
  
  // cols, all names and values are decoded.
  this.cols = null; // list of hashes of {name, value};
  this.colHash = null; // hash of  name->value
};

// todo: would it be more idiomatic to have next() take a callback that passes in the row?
/** advances the cursor and loads the next row. returns true or false depending on whether or not the cursor is over a row. */
Results.prototype.next = function() {
  if (!this.rows[this.ptr]) {
    this.key = null;
    this.cols = null;
    return false;
  } else {
    this.key = this.decoder.decode(this.rows[this.ptr].key, 'key');
    this.cols = []; // this.rows[this.ptr].columns;
    this.colHash = {};
    
    for (var i = 0; i < this.rows[this.ptr].columns.length; i++) {
      var decodedName = this.decoder.decode(this.rows[this.ptr].columns[i].name, 'comparator');
      // notice how we pass the raw bytes of the column name into the decoder.  This might be the wrong assumption (look
      // in the Connection construtor where the validator is built.
      var decodedValue = this.decoder.decode(this.rows[this.ptr].columns[i].value, 'validator', this.rows[this.ptr].columns[i].name);
      this.cols[i] = {
        name: decodedName,
        value: decodedValue
      };
      this.colHash[decodedName] = decodedValue;
    }
    this.ptr += 1;
    return true;
  }
};

/** ask for a column by index. returns the full column (you have .name, .value, .timestamp, .ttl). */
Results.prototype.getByIndex = function(i) {
  if (!this.key) {
    throw new Error('Results cursor not positioned at a row');
  } else if (!this.cols[i]) {
    throw new Error('Invalid column index: ' + i);
  } else {
    return {name: this.cols[i].name, value: this.cols[i].value};
  }
};

/** asks for the value of a column by its name. returns the value only. */
Results.prototype.getByName = function(name) {
  if (!this.key) {
    throw new Error('Results cursor not positioned at a row');
  } else if (!this.colHash[name]) {
    throw new Error('Column with name not present: ' + name);
  } else {
    return this.colHash[name];
  }
};

/** ask for the number of columns in the current row. */
Results.prototype.colCount = function() {
  if (!this.key) {
    throw new Error('Results cursor not positioned at a row');
  } else {
    return this.cols.length;
  }
};




/** where query work is performed. wraps thrift connection abstraction. provides a queue to process query requests. */
Connection = module.exports.Connection = function(user, pass, host, port, keyspace) {
  EventEmitter.call(this);
  var self = this;
  this.q = new Queue(10000);
  this.connection = new DriverThriftConnection(host, port);
  this.validators = {};
  
  // start the queue processor.
  this.on('checkq', function() {
    if (self.connection.isClosed) {
      return;
    }
    if (!self.connection.isConnected) {
      self.connection.connector();
      // no connection is available. create a timer event to check back in a bit to see if there is a con available to
      // do work.
      setTimeout(function() {
        self.emit('checkq');
      }, 25);
    } else {
      // drain the work queue.
      while (self.q.size() > 0) {
        // each function in the queue accepts a thrift client.
        if (self.connection.isConnected) {
          self.q.take()(self.connection.tclient);
        } else {
          console.error('connection is buggered');
        }
      }
    }
  });
  this.connection.connector();
  
  // maybe login.
  if (user || pass) {
    this.q.put(function(con) {
      var creds = new ttypes.AuthenticationRequest({user: user, password: pass});
      con.login(creds, function(err) {
        if (err) {
          throw new Error(err);
        }
      });
    });
    this.emit('checkq');
  }
  
  // learn about the keyspace
  this.q.put(function(con) {
    con.describe_keyspace(keyspace, function(err, ksDef) {
      if (err) {
        throw new Error(err);
      } else {
        // we need to note the comparators/validators for proper type conversion on queries.
        for (var i = 0; i < ksDef.cf_defs.length; i++) {
          var validators = {
            key: ksDef.cf_defs[i].key_validation_class,
            comparator: ksDef.cf_defs[i].comparator_type,
            defaultValidator: ksDef.cf_defs[i].default_validation_class,
            specificValidators: {}
          };
          for (var j = 0; j < ksDef.cf_defs[i].column_metadata.length; j++) {
            // todo: verify that the name we use as the key represents the raw-bytes version of the column name, not the 
            // stringified version.
            validators.specificValidators[ksDef.cf_defs[i].column_metadata[j].name] = ksDef.cf_defs[i].column_metadata[j].validation_class; 
          }
          self.validators[ksDef.cf_defs[i].name] = validators;
        }
      }
    });
  });
  
  // set the keyspace.
  this.q.put(function(con) {
    con.set_keyspace(keyspace, function(err) {
      if (err) {
        throw new Error(err);
      }
    });
  });
  this.emit('checkq');
};
sys.inherits(Connection, EventEmitter);

/** close the connection. */
Connection.prototype.close = function() {
  var self = this;
  this._putWork(function(c) {
    self.connection.close();
  });
};

/** create a Statement */
Connection.prototype.createStatement = function() {
  return new Statement(this);
};

// puts work (a query request) on the queue. fn(ThriftConnection.tclient)
Connection.prototype._putWork = function(fn) {
  this.q.put(fn);
  this.emit('checkq');
};


