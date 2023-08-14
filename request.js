var getConnection = require('./get-connection');
var ezuuid = require('ezuuid');
var _ = require('lodash');

function initChannel (connString) {
	return getConnection(connString)
		.then(function(conn) {
			return conn.createChannel();
		})
		.then(function(chan){
			var options = { noAck: true };
			const myHandler = _.partial(handleResponse, connString);
			return chan.consume('amq.rabbitmq.reply-to', myHandler, options)
				.then(function(){
					return chan.assertExchange('_rpc_send_direct', 'direct', { durable: true });
				})
				.then(function(){
					return chan;
				});
		})
		.catch(function(err){
			console.log('error initializing channel');
			console.error(err);
		});
}

const channelDict = {};

function handleResponse(connString, response){
	if (!response || !response.properties || !response.properties.correlationId){
		return console.dir('error, bad response.', response);
	}

	var conn = connString ? connString : 'main';
	var correlationId = response.properties.correlationId;
	var requestEntry = requestLookup[conn][correlationId];

	if (!requestEntry){
		return console.dir('error, unknown correlationId.');
	}

	clearTimeout(requestEntry.timeout);

	var msg = JSON.parse(response.content.toString());
	delete requestLookup[conn][correlationId];

	if (msg && msg._rpcError) {
		requestEntry.cb(new Error(msg._message || 'unknown error in rpc server'));
	} else {
		requestEntry.cb(null, msg);
	}
}

var DEFAULTS = {timeout: 3000};
function createOptions(methodName, options){
	switch (typeof methodName){
		case 'string':
			options = Object(options);
			options.methodName = methodName;
			break;
		case 'object':
			options = methodName;
	}

	methodName = options.methodName;
	options = _.extend({}, DEFAULTS, options);
	return options;
}

var requestLookup = {
	main: {},
};
function request (connString){
	var options = createOptions.apply(null, _.toArray(arguments).slice(1));
	var methodName = options.methodName;

	return function(req = {}, cb){
		var conn = connString ? connString : 'main';
		var correlationId = ezuuid();

		requestLookup[conn] = requestLookup[conn] || {};
		var requestEntry = requestLookup[conn][correlationId] = {
			cb: cb,
		};

		if (!channelDict[conn]) {
			channelDict[conn] = initChannel(connString);
		}

		return channelDict[conn]
			.then(function(chan){
				if (!chan){
					console.error('unable to get initialized channel');
					delete requestLookup[conn][correlationId];
					return cb(new Error('unable to initialize rpc channel'));
				}

				var options = {
					key: methodName,
					correlationId: correlationId,
					persistent: false,
					replyTo: 'amq.rabbitmq.reply-to',
					contentType: 'application/json',
				};

				return chan.publish('_rpc_send_direct', methodName, new Buffer(JSON.stringify(req)), options);
			})
			.then(function(){
				requestEntry.timeout = setTimeout(function(){
					delete requestLookup[conn][correlationId];
					cb(new Error('timeout'));
				}, options.timeout);
			})
			.catch(function(err){
				console.log('error sending request: ', methodName);
				console.error(err);
			});
	};
}

module.exports = function (opt = {}) {
	return _.partial(request, opt.connString);
};

module.exports.createOptions = createOptions;
