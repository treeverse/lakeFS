# js-md5
[![Build Status](https://travis-ci.org/emn178/js-md5.svg?branch=master)](https://travis-ci.org/emn178/js-md5)
[![Coverage Status](https://coveralls.io/repos/emn178/js-md5/badge.svg?branch=master)](https://coveralls.io/r/emn178/js-md5?branch=master)  
[![NPM](https://nodei.co/npm/js-md5.png?stars&downloads)](https://nodei.co/npm/js-md5/)

A simple and fast MD5 hash function for JavaScript supports UTF-8 encoding.

## Demo
[MD5 Online](http://emn178.github.io/online-tools/md5.html)  
[MD5 File Checksum Online](http://emn178.github.io/online-tools/md5_checksum.html)

## Download
[Compress](https://raw.github.com/emn178/js-md5/master/build/md5.min.js)  
[Uncompress](https://raw.github.com/emn178/js-md5/master/src/md5.js)

## Benchmark
[jsPerf Benchmark](https://jsperf.app/jonuhi)  
[File Benchmark](https://github.com/emn178/js-md5/issues/19)

## Installation
You can also install js-md5 by using Bower.

    bower install md5

For node.js, you can use this command to install:

    npm install js-md5

## Notice
`buffer` method is deprecated. This maybe confuse with Buffer in node.js. Please use `arrayBuffer` instead.

## Usage
You could use like this:
```JavaScript
md5('Message to hash');
var hash = md5.create();
hash.update('Message to hash');
hash.hex();

// HMAC
md5.hmac('key', 'Message to hash');

var hash = md5.hmac.create('key');
hash.update('Message to hash');
hash.hex();
```

### Node.js
If you use node.js, you should require the module first:
```JavaScript
var md5 = require('js-md5');
```

### TypeScript
If you use TypeScript, you can import like this:
```TypeScript
import { md5 } from 'js-md5';
```

## RequireJS
It supports AMD:
```JavaScript
require(['your/path/md5.js'], function(md5) {
// ...
});
```
[See document](https://emn178.github.com/js-md5/doc/)

## Example
```JavaScript
md5(''); // d41d8cd98f00b204e9800998ecf8427e
md5('The quick brown fox jumps over the lazy dog'); // 9e107d9d372bb6826bd81d3542a419d6
md5('The quick brown fox jumps over the lazy dog.'); // e4d909c290d0fb1ca068ffaddf22cbd0

// It also supports UTF-8 encoding
md5('中文'); // a7bac2239fcdcb3a067903d8077c4a07

// It also supports byte `Array`, `Uint8Array`, `ArrayBuffer`
md5([]); // d41d8cd98f00b204e9800998ecf8427e
md5(new Uint8Array([])); // d41d8cd98f00b204e9800998ecf8427e

// Different output
md5(''); // d41d8cd98f00b204e9800998ecf8427e
md5.hex(''); // d41d8cd98f00b204e9800998ecf8427e
md5.array(''); // [212, 29, 140, 217, 143, 0, 178, 4, 233, 128, 9, 152, 236, 248, 66, 126]
md5.digest(''); // [212, 29, 140, 217, 143, 0, 178, 4, 233, 128, 9, 152, 236, 248, 66, 126]
md5.arrayBuffer(''); // ArrayBuffer
md5.buffer(''); // ArrayBuffer, deprecated, This maybe confuse with Buffer in node.js. Please use arrayBuffer instead.
md5.base64(''); // 1B2M2Y8AsgTpgAmY7PhCfg==

// HMAC
md5.hmac.hex('key', 'Message to hash');
md5.hmac.array('key', 'Message to hash');
// ...
```

## License
The project is released under the [MIT license](https://opensource.org/license/mit/).

## Contact
The project's website is located at https://github.com/emn178/js-md5  
Author: Chen, Yi-Cyuan (emn178@gmail.com)
