/*
 *  Copyright 2011 Rackspace
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/** node.js driver for Cassandra-CQL. */

var log = require('logmagic').local('node-cassandra-client.driver');
var sys = require('sys');
var EventEmitter = require('events').EventEmitter;

var thrift = require('thrift');
var Cassandra = require('./gen-nodejs/Cassandra');
var ttypes = require('./gen-nodejs/cassandra_types');

var genericPool = require('generic-pool');

var Decoder = require('./decoder').Decoder;

// used to parse the CF name out of a select statement.
var selectRe = /\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)/im;

var appExceptions = ['InvalidRequestException', 'TimedOutException', 'UnavailableException',
  'SchemaDisagreementException'];

var nullBindError = {
  message: 'null/undefined query parameter'
};

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

/**
 * binds arguments to a query. e.g: bind('select ?, ? from MyCf where key=?', ['arg0', 'arg1', 'arg2']);
 * quoting is handled for you.  so is converting the parameters to a string, preparatory to being sent up to cassandra.
 * @param query
 * @param args array of arguments. falsy values are never acceptable.
 * @result a string suitable for cassandra.execute_cql_query().
 */
function bind(query, args) {
  if (args.length === 0) {
    return query;
  }
  var q = 0;
  var a = 0;
  var str = '';
  while (q >= 0) {
    var oldq = q;
    q = query.indexOf('?', q);
    if (q >= 0) {
      str += query.substr(oldq, q-oldq);
      if (args[a] === null) {
        return nullBindError;
      }
      str += quote(fixQuotes(stringify(args[a++])));
      q += 1;
    } else {
      str += query.substr(oldq);
    }
  }
  return str;
}

/** returns true if obj is in the array */
function contains(a, obj) {
  var i = a.length;
  while (i > 0) {
    if (a[i-1] === obj) {
      return true;
    }
    i--;
  }
  return false;
}


System = module.exports.System = require('./system').System;
KsDef = module.exports.KsDef = require('./system').KsDef;
CfDef = module.exports.CfDef = require('./system').CfDef;
ColumnDef = module.exports.ColumnDef = require('./system').ColumnDef;
BigInteger = module.exports.BigInteger = require('./bigint').BigInteger;
UUID = module.exports.UUID = require('./uuid').UUID;



/** abstraction of a single row. */
Row = module.exports.Row = function(row, decoder) {
  // decoded key.
  this.key = decoder.decode(row.key, 'key');
  
  // cols, all names and values are decoded.
  this.cols = []; // list of hashes of {name, value};
  this.colHash = {}; // hash of  name->value
  
  var count = 0;
  for (var i = 0; i < row.columns.length; i++) {
    if (row.columns[i].value) {
      var decodedName = decoder.decode(row.columns[i].name, 'comparator');
      var decodedValue = decoder.decode(row.columns[i].value, 'validator', row.columns[i].name);
      this.cols[count] = {
        name: decodedName,
        value: decodedValue
      };
      this.colHash[decodedName] = decodedValue;
      count += 1;
    }
  }
  
  this._colCount = count;
};

/** @returns the number of columns in this row. */
Row.prototype.colCount = function() {
  return this._colCount;
};

/**
 * Perform queries against a pool of open connections.
 * 
 * Accepts a single argument of an object used to configure the new PooledConnection
 * instance.  The config object supports the following attributes:
 * 
 *         hosts : List of strings in host:port format.
 *      keyspace : Keyspace name.
 *          user : User for authentication (optional).
 *          pass : Password for authentication (optional).
 *       maxSize : Maximum number of connection to pool (optional).
 *    idleMillis : Idle connection timeout in milliseconds (optional).
 * 
 * Example:
 * 
 *   var pool = new PooledConnection({
 *     hosts      : ['host1:9160', 'host2:9170', 'host3', 'host4'],
 *     keyspace   : 'database',
 *     user       : 'mary',
 *     pass       : 'qwerty',
 *     maxSize    : 25,
 *     idleMillis : 30000
 *   });
 * 
 * @param config an object used to control the creation of new instances.
 */
PooledConnection = module.exports.PooledConnection = function(config) {
  this.nodes = [];
  this.holdFor = 10000;
  this.current_node = 0;
  this.config = config;
  
  // Construct a list of nodes from hosts in <host>:<port> form
  for (var i = 0; i < config.hosts.length; i++) {
    hostSpec = config.hosts[i];
    if (!hostSpec) { continue; }
    host = hostSpec.split(':');
    if (host.length > 2) {
      log.warn('malformed host entry "' + hostSpec + '" (skipping)');
    }
    log.debug("adding " + hostSpec + " to working node list");
    this.nodes.push([host[0], (isNaN(host[1])) ? 9160 : host[1]]);
  }
  
  var self = this;
  var maxSize = isNaN(config.maxSize) ? 25 : config.maxsize;
  var idleMillis = isNaN(config.idleMillis) ? 30000 : config.idleMillis;
  
  this.pool = genericPool.Pool({
    name    : 'Connection',
    create  : function(callback) {
      // Advance through the set of configured nodes
      if ((self.current_node + 1) >= self.nodes.length) {
        self.current_node = 0;
      } else {
        self.current_node++;
      }
      
      var tries = self.nodes.length;
	    
	    function retry(curNode) {
	      tries--;
	      
	      if ((curNode + 1) >= self.nodes.length) {
          curNode = 0;
        } else {
          curNode++;
        }

	      var node = self.nodes[curNode];
	      // Skip over any nodes known to be bad
	      if (node.holdUntil > (new Date().getTime())) {
	        return retry(curNode);
	      }
	      
	      var conn = new Connection(node[0], node[1], config.keyspace, config.user, config.pass);
	      
	      conn.connect(function(err) {
	        if (!err) {                   // Success, we're connected
	          callback(conn);
	        } else if (tries > 0) {       // Fail, mark node inactive and retry
	          log.err("Unabled to connect to " + node[0] + ":" + node[1] + " (skipping)");
	          node.holdUntil = new Date().getTime() + self.holdFor;
	          retry(curNode);
	        } else {                      // Exhausted all options
	          callback(null);
	        }
	      });
	    }
	    retry(self.current_node);
	  },
	  destroy : function(conn) { conn.close(); },
	  max     : maxSize,
	  idleTimeoutMillis : idleMillis,
	  log : true
  });
};

/**
 * executes any query
 * @param query any CQL statement with '?' placeholders.
 * @param args array of arguments that will be bound to the query.
 * @param callback executed when the query returns. the callback takes a different number of arguments depending on the
 * type of query:
 *    SELECT (single row): callback(err, row)
 *    SELECT (mult rows) : callback(err, rows)
 *    SELECT (count)     : callback(err, count)
 *    UPDATE             : callback(err)
 *    DELETE             : callback(err)
 */
PooledConnection.prototype.execute = function(query, args, callback) {
  var self = this;
  var seen = false;
  
  var exe = function(errback) {
    self.pool.acquire(function(conn) {
      if (!conn) {
        callback(Error('Unable to acquire an open connection from the pool!'));
      } else {
        conn.execute(query, args, function(err, res) {
          if (err) {
            if (contains(appExceptions, err.name)) {
              self.pool.release(conn);
              callback(err, null);
            } else {
              if (!seen) {
                errback();
              } else {
                self.pool.release(conn);
                callback(err, null);
              }
            }
          } else {
            self.pool.release(conn);
            callback(err, res);
          }
        });
      }
    });
  };
  
  var retry = function() {
    seen = true;
    exe(retry);
  };
  
  exe(retry);
};

/**
 * Signal the pool to shutdown.  Once called, no new requests (read: execute())
 * can be made. When all pending requests have terminated, the callback is run.
 * 
 * @param callback called when the pool is fully shutdown
 */
PooledConnection.prototype.shutdown = function(callback) {
  var self = this;
  this.pool.drain(function() {
    self.pool.destroyAllNow(callback);
  });
};

/**
 * @param user
 * @param pass
 * @param host
 * @param port
 * @param keyspace
 */
Connection = module.exports.Connection = function(host, port, keyspace, user, pass) {
  log.info('connecting ' + host + ':' + port);
  this.validators = {};
  this.con = thrift.createConnection(host, port);
  this.client = null;
  this.connectionInfo = {
    user: user,
    pass: pass,
    host: host,
    port: port,
    keyspace: keyspace
  };
};

function ensureError(err) {
  if (!err) {
    return err;
  } else if (!err.name || err.name !== 'Error') {
    return new Error(err);
  } else {
    return err;
  }
}

/**
 * makes the connection. 
 * @param callback called when connection is successful or ultimately fails (err will be present).
 */
Connection.prototype.connect = function(callback) {
  var self = this;
  this.con.on('error', function(err) {
    callback(ensureError(err));
  });
  this.con.on('close', function() {
    log.info(self.connectionInfo.host + ':' + self.connectionInfo.port + ' is closed');
  });
  this.con.on('connect', function() {
    // preparing the conneciton is a 3-step process.
    
    // 1) login
    var login = function(cb) {
      if (self.connectionInfo.user || self.connectionInfo.pass) {
        var creds = new ttypes.AuthenticationRequest({user: self.connectionInfo.user, password: self.connectionInfo.pass});
        self.client.login(creds, function(err) {
          cb(ensureError(err));
        });
      } else {
        cb(null);
      }
    };
    
    // 2) login.
    var learn = function(cb) {
      self.client.describe_keyspace(self.connectionInfo.keyspace, function(err, def) {
        if (err) {
          cb(ensureError(err));
        } else {
          for (var i = 0; i < def.cf_defs.length; i++) {
            var validators = {
              key: def.cf_defs[i].key_validation_class,
              comparator: def.cf_defs[i].comparator_type,
              defaultValidator: def.cf_defs[i].default_validation_class,
              specificValidators: {}
            };
            for (var j = 0; j < def.cf_defs[i].column_metadata.length; j++) {
              // todo: verify that the name we use as the key represents the raw-bytes version of the column name, not 
              // the stringified version.
              validators.specificValidators[def.cf_defs[i].column_metadata[j].name] = def.cf_defs[i].column_metadata[j].validation_class;
            }
            self.validators[def.cf_defs[i].name] = validators;
          }
          cb(null); // no errors.
        }
      });
    };
    
    // 3) set the keyspace on the server.
    var use = function(cb) {
      self.client.set_keyspace(self.connectionInfo.keyspace, function(err) {
        cb(ensureError(err));
      });
    };
    
    // put it all together, checking for errors along the way.
    login(function(loginErr) {
      if (loginErr) {
        callback(loginErr);
        self.close();
      } else {
        learn(function(learnErr) {
          if (learnErr) {
            callback(learnErr);
            self.close();
          } else {
            use(function(useErr) {
              if (useErr) {
                callback(useErr);
                self.close();
              } else {
                // this connection is finally ready to use.
                callback(null);
              }
            });
          }
        });
      }
    });
    
  });
  
  // kicks off the connection process.
  this.client = thrift.createClient(Cassandra, this.con);
};

Connection.prototype.close = function() {
  this.con.end();
  this.con = null;
  this.client = null;
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
  if (cql === nullBindError) {
    callback(ensureError(nullBindError.message), null);
  } else {
    var self = this;
    this.client.execute_cql_query(cql, ttypes.Compression.NONE, function(err, res) {
      if (err) {
        callback(ensureError(err), null);
      } else if (!res) {
        callback(new Error('No results'), null);
      } else {
        if (res.type === ttypes.CqlResultType.ROWS) {
          var cfName = selectRe.exec(cql)[1];
          var decoder = new Decoder(self.validators[cfName]);
          // for now, return results.
          var rows = [];
          for (var i = 0; i < res.rows.length; i++) {
            var row = new Row(res.rows[i], decoder);
            rows.push(row);
          }
          rows.rowCount = function() {
            return res.rows.length;
          };
          callback(null, rows);
        } else if (res.type === ttypes.CqlResultType.INT) {
          callback(null, res.num);
        } else if (res.type === ttypes.CqlResultType.VOID) {
          callback(null);
        }
      }
    }); 
  }
};
