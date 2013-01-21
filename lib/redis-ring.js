/* redis-ring
 * A node_redis wrapper that adds node-hash-ring support.
 * (c) 2013 David (daXXog) Volm ><> + + + <><
 * Released under Apache License, Version 2.0:
 * http://www.apache.org/licenses/LICENSE-2.0.html  
 */

/* UMD LOADER: https://github.com/umdjs/umd/blob/master/returnExports.js */
(function (root, factory) {
    if (typeof exports === 'object') {
        // Node. Does not work with strict CommonJS, but
        // only CommonJS-like enviroments that support module.exports,
        // like Node.
        module.exports = factory();
    } else if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define(factory);
    } else {
        // Browser globals (root is window)
        root.returnExports = factory();
  }
}(this, function() {
    var redis = require('redis'),
        HashRing = require('hash_ring');
    
    var RedisRing = function(ring, options, auth) {
        var that = this,
            _prefix = 'key_',
            _client = redis.createClient(),
            _proto = Object.getPrototypeOf(_client),
            _protoList = [],
            _ring = {},
            clientSpawners = [],
            clients = [];
            
        _client.quit();
        
        var _firstKey = function(obj) {
            for(var key in obj) {
                return key;
            }
            
            return -1;
        };
        
        var _to_array = function(args) {
            var len = args.length,
                arr = new Array(len), i;
        
            for (i = 0; i < len; i += 1) {
                arr[i] = args[i];
            }
        
            return arr;
        };
        
        ring.forEach(function(v, i, a) {
            var _k = _firstKey(v),
                __k = _k.split(':'),
                weight = v[_k],
                host = __k[0],
                port = __k[1];
                
            var _id = clientSpawners.push(((function(port, host) {
                return function(port, host) {
                    return redis.createClient(port, host, options);
                };
            })(port, host)));
            
            clients.push(clientSpawners[_id - 1]());
            _ring[_id - 1] = weight;
        });
        
        this.hashRing = new HashRing(_ring);
        
        this._cmd = function(cmd, args) {
            _proto[cmd].apply(clients[that.hashRing.getNode(_prefix + args[0])], args);
        };
        
        for(var k in _proto) {
            _protoList.push(k);
        }
        
        var exclude_f = [
            'initialize_retry_vars',
            'flush_and_error',
            'on_error',
            'do_auth',
            'on_connect',
            'init_parser',
            'on_ready',
            'on_info_cmd',
            'ready_check',
            'send_offline_queue',
            'connection_gone',
            'on_data',
            'return_error',
            'return_reply',
            'pub_sub_command',
            'send_command',
            
            'quit',
            'subscribe',
            
            'setMaxListeners',
            'emit',
            'addListener',
            'on',
            'once',
            'removeListener',
            'removeAllListeners',
            'listeners',
        ];
        
        _protoList.forEach(function(v, i, a) {
            if(exclude_f.indexOf(v) === -1) {
                that[v] = function() {
                    return that._cmd(v, _to_array(arguments));
                }
            }
        });
        
        this.quit = function() {
            clients.forEach(function(v, i, a) {
                v.quit();
            });
        };
        
        this.spawnClient = function(key) {
            return clientSpawners[that.hashRing.getNode(_prefix + key)]();
        };
        
        this.subscribe = function(channel, cb) {
            var client = that.spawnClient(channel);
            
            client.on('message', function(_channel, message) {
                if(_channel === channel) {
                    cb(message);
                }
            });
            
            client.subscribe(channel);
            
            return client;
        };
    };
    
    return RedisRing;
}));