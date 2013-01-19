/* redis-ring / example.js
 * Examples for redis-ring
 * (c) 2013 David (daXXog) Volm ><> + + + <><
 * Released under Apache License, Version 2.0:
 * http://www.apache.org/licenses/LICENSE-2.0.html  
 */

var RedisRing = require('./index.js');

var ring = [
        {"127.0.0.1:6379": 1}
    ],
    client = new RedisRing(ring);

client.set('hello', 'world');

client.quit();