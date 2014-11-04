var debug = require('debug')('skipper-gcs');
var fs = require('fs');
var jwt = require('jsonwebtoken');
var request = require('request');
var __cache = {};

// constants
var GOOGLE_OAUTH2_URL = 'https://accounts.google.com/o/oauth2/token';

exports.get = function(options, cb) {
  var iat = Math.floor(Date.now() / 1000);
  var exp = iat + Math.floor((options.expiration || 60 * 60 * 1000) / 1000);
  var payload = {
    iss: options.email,
    scope: options.scopes.join(' '),
    aud: GOOGLE_OAUTH2_URL,
    exp: exp,
    iat: iat
  };
  debug('request payload', payload);
  // sign with RSA SHA256
  var cert = fs.readFileSync(options.keyFile); // get private key
  var token = jwt.sign(payload, cert, { algorithm: 'RS256' });
  request.post(GOOGLE_OAUTH2_URL, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    form: {
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: token
    }
  }, function(err, res, body) {
    if (err) {
      return cb(err);
    } else {
      debug('response from OAuth server');
      debug('http status', res.statusCode);
      debug('body', body);
    }
    if (res.statusCode != 200) {
      err = new Error(
        'failed to obtain an authentication token, request failed with HTTP code ' +
        res.statusCode + ': ' + body.error
      );
      err.statusCode = res.statusCode;
      err.body = body;
      return cb(err);
    }
    try {
      body = JSON.parse(body);
    } catch (e) {
      return cb(new Error('failed to parse response body: ' + body));
    }
    body.exp = exp * 1000;
    return cb(null, body);
  });
},

exports.cache = function(options, cb) {
  var key = options.email + ':' + options.scopes.join(',');
  debug('dump cache ', __cache);
  if (__cache[key] && __cache[key].exp > Date.now()) {
    debug('token cache hit');
    cb(null, __cache[key]);
  } else {
    debug('token cache missing');
    this.get(options, function(err, token) {
      if (err) {
        cb(err);
      } else {
        __cache[key] = token;
        cb(null, token);
      }
    }); 
  }
}