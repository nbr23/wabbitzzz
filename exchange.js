var Promise = require('bluebird'),
	util = require('util'),
	getConnection = require('./get-connection'),
	_ = require('lodash'),
	EventEmitter = require('events').EventEmitter,
	defaultExchangePublish = require('./default-exchange-publish');

const { serialize } = require('./serializer.js');

var EXCHANGE_DEFAULTS = {
	type: 'fanout',
	autoDelete: false,
	durable: true,
	name: '',
};

var PUBLISH_DEFAULTS = {
	persistent: true,
	contentType: 'application/json',
};

var DELAYED_PUBLISH_DEFAULTS = {
	delay: 3000,
	key: '',
};

var assertedQueues = {};

function _createChannel(connString, confirmMode){
	return getConnection(connString)
		.then(function(conn) {
			if (confirmMode){
				return conn.createConfirmChannel();
			}

			return conn.createChannel();
		});
}

function _assertExchange(channel, params) {
	// if using the default exchange,
	// then skip the assert
	if (!params.name) return channel;

	var opt = _.cloneDeep(params);
	delete opt.name;
	delete opt.type;

	return channel.assertExchange(params.name, params.type, opt)
		.then(function() {
			return channel;
		});
}

const channelDict = {};

function Exchange(connString, params){
	var self = this;
	EventEmitter.call(self);
	params = _.extend({}, EXCHANGE_DEFAULTS, params);
	connString = connString || 'main';

	var delayAssertChannel;
	var confirmMode = !!params.confirm;
	delete params.confirm;

	var exchangeName = params.name || '';
	var getChannel;
	if (confirmMode) {
		getChannel = _createChannel(connString, true);
	} else {
		getChannel = channelDict[connString];
	}

	getChannel = getChannel
		.then(function(c) { return _assertExchange(c, params); });

	var property = Object.defineProperty.bind(Object, self);

	getChannel
		.then(function(){
			self.emit('ready');
		})
		.catch(function(err){
			console.log('error creating exchange');
			console.log(err);
		});

	property('ready', {
		get: function(){ return getChannel; },
	});

	this.publish = function(msg, publishOptions, cb){
		// make sure we arent publishing fancy mongoose objects
		if (msg && _.isFunction(msg.toObject)) {
			msg = msg.toObject();
		}

		return getChannel
			.then(function(chan){

				const options = _.extend({}, PUBLISH_DEFAULTS, publishOptions);
				const key = (options.key || 'blank').toString();

				msg._exchange = msg._exchange || exchangeName;

				const { contentType = 'application/json' } = options;
				const buf = serialize(msg, contentType);

				delete options.key;


				if (confirmMode){
					chan.publish(exchangeName, key, buf, options);
					return chan.waitForConfirms()
						.then(function(){
							if (_.isFunction(cb)) cb();
							return true;
						});
				}

				return chan.publish(exchangeName, key, buf, options);
			});
	};

	this.delayedPublish = function(msg, publishOptions){
		// make sure we arent publishing fancy mongoose objects
		if (msg && _.isFunction(msg.toObject)) {
			msg = msg.toObject();
		}

		publishOptions = _.extend({}, DELAYED_PUBLISH_DEFAULTS, publishOptions);
		var skipQueueAssert = publishOptions.skipQueueAssert;
		delete publishOptions.skipQueueAssert;

		msg._exchange = msg._exchange || exchangeName;

		// negative delays break things
		var delay = Math.max(publishOptions.delay, 1);
		var queueName = 'delay_' + exchangeName  +'_by_'+publishOptions.delay+'__'+publishOptions.key;
		var promise;

		if (skipQueueAssert) {
			queueName += '_skip';
		}

		if (skipQueueAssert && assertedQueues[queueName]) {
			promise = Promise.resolve();
			// console.log('SKIP assert', queueName);
		} else {
			if (!delayAssertChannel) {
				delayAssertChannel = _createChannel(connString);
			}
			// console.log('assert', queueName);

			promise = delayAssertChannel
				.then(function(chan) {

					var options = {
						exclusive: false,
						autoDelete: false,
						arguments: {
							'x-dead-letter-exchange': exchangeName,
							'x-dead-letter-routing-key': publishOptions.key,
							'x-message-ttl': delay,
						},
					};

					// if we are going to skip the assert then we need to 
					// not automatically delete the queue
					if (skipQueueAssert) {
						options.arguments['x-rabbit-pal-remove'] = 'ignore';
					}

					assertedQueues[queueName] = true;
					return chan.assertQueue(queueName, options);
				});
		}

		return promise
			.then(function() {
				return defaultExchangePublish(connString, msg, { key: queueName });
			})
			.then(function() {
				return true;
			});
	};
}

util.inherits(Exchange, EventEmitter);

module.exports = function (opt = {}) {
	const { connString = 'main' } = opt;

	if (!channelDict[connString]) {
		channelDict[connString] = _createChannel(connString);
	}

	return _.partial(Exchange, opt.connString);
};
