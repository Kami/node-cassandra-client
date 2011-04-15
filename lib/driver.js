
/** node.js driver for Cassandra-CQL. */

var console = require('console');
var sys = require('sys');
var EventEmitter = require('events').EventEmitter;

var thrift = require('thrift');
var Cassandra = require('./gen-nodejs/Cassandra');
var ttypes = require('./gen-nodejs/cassandra_types');

var Decoder = require('./decoder').Decoder;

// used to parse the CF name out of a select statement.
var selectRe = /\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)/im;

/**
 * binds arguments to a query. e.g: bind('select ?, ? from MyCf where key=?', ['arg0', 'arg1', 'arg2']);
 * quoting is handled for you.  so is converting the parameters to a string, preparatory to being sent up to cassandra.
 * @param query
 * @param args array of arguments.
 * @result a string suitable for cassandra.execute_cql_query().
 */
function bind(query, args) {
  var q = 0;
  var a = 0;
  var str = '';
  while (q >= 0) {
    var oldq = q;
    q = query.indexOf('?', q);
    if (q >= 0) {
      str += query.substr(oldq, q-oldq);
      str += quote(fixQuotes(stringify(args[a++])));
      q += 1;
    } else {
      str += query.substr(oldq);
    }
  }
  return str;
}

/** converts object to a string using toString() method if it exists. */
function stringify(x) {
  if (x.toString) {
    return x.toString();
  } else {
    return x;
  }
}

/** wraps in quotes */
function quote(x) {
  return '\'' + x + '\'';
}

/** replaces single quotes with double quotes */
function fixQuotes(x) {
  return x.replace(/\'/img, '\'\'');
}

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



/** abstraction of a single row. */
Row = module.exports.Row = function(row, decoder) {
  // decoded key.
  this.key = decoder.decode(row.key, 'key');
  
  // cols, all names and values are decoded.
  this.cols = []; // list of hashes of {name, value};
  this.colHash = {}; // hash of  name->value
  
  for (var i = 0; i < row.columns.length; i++) {
    var decodedName = decoder.decode(row.columns[i].name, 'comparator');
    var decodedValue = decoder.decode(row.columns[i].value, 'validator', row.columns[i].name);
    this.cols[i] = {
      name: decodedName,
      value: decodedValue
    };
    this.colHash[decodedName] = decodedValue;
  }
  
  this._colCount = i;
};

/** @returns the number of columns in this row. */
Row.prototype.colCount = function() {
  return this._colCount;
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

/**
 * executes any query
 * @param query any cql statement with '?' placeholders.
 * @param args array of arguments that will be bound to the query.
 * @param callback executed when the query returns. the callback takes a different number of arguments depending on the
 * type of query:
 *    SELECT (single row): callback(err, row)
 *    SELECT (mult rows) : callback(err, rows)
 *    SELECT (count)     : callback(err, count)
 *    UPDATE             : callback(err)
 *    DELETE             : callback(err)
 */
Connection.prototype.execute = function(query, args, callback) {
  var cql = bind(query, args);
  var self = this;
  this._putWork(function(c) {
    c.execute_cql_query(cql, ttypes.Compression.NONE, function(err, res) {
      if (err) {
        callback(err, null);
      } else if (!res) {
        callback('No results', null);
      } else {
        if (res.type === ttypes.CqlResultType.ROWS) {
          var cfName = selectRe.exec(cql)[1];
          var decoder = new Decoder(self.validators[cfName]);
          // for now, return results.
          if (res.rows.length === 1) {
            callback(null, new Row(res.rows[0], decoder));
          } else {
            var rows = {};
            for (var i = 0; i < res.rows.length; i++) {
              var row = new Row(res.rows[i], decoder);
              rows[row.key] = row;
            }
            callback(null, rows);
          }
        } else if (res.type === ttypes.CqlResultType.INT) {
          callback(null, res.num);
        } else if (res.type === ttypes.CqlResultType.VOID) {
          callback(null);
        }
      }
    });
  });
};

// puts work (a query request) on the queue. fn(ThriftConnection.tclient)
Connection.prototype._putWork = function(fn) {
  this.q.put(fn);
  this.emit('checkq');
};
