
/** node.js driver for Cassandra-CQL. */

var log = require('logmagic').local('node-cassandra-client.driver');
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
 * @param user
 * @param pass
 * @param host
 * @param port
 * @param keyspace
 */
Connection = module.exports.Connection = function(user, pass, host, port, keyspace) {
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

/**
 * makes the connection. 
 * @param callback called when connection is successful or ultimately fails (err will be present).
 */
Connection.prototype.connect = function(callback) {
  var self = this;
  this.con.on('error', function(err) {
    callback(err);
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
          cb(err);
        });
      } else {
        cb(null);
      }
    };
    
    // 2) login.
    var learn = function(cb) {
      self.client.describe_keyspace(self.connectionInfo.keyspace, function(err, def) {
        if (err) {
          cb(err);
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
        }
        cb(null); // no errors.
      });
    };
    
    // 3) set the keyspace on the server.
    var use = function(cb) {
      self.client.set_keyspace(self.connectionInfo.keyspace, function(err) {
        cb(err);
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
  var self = this;
  this.client.execute_cql_query(cql, ttypes.Compression.NONE, function(err, res) {
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
          rows.rowCount = function() {
            return res.rows.length;
          };
          callback(null, rows);
        }
      } else if (res.type === ttypes.CqlResultType.INT) {
        callback(null, res.num);
      } else if (res.type === ttypes.CqlResultType.VOID) {
        callback(null);
      }
    }
  });
};
