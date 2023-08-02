var Promise = require('bluebird'),
	util = require('util'),
	getConnection = require('./get-connection'),
	queue = require('./queue'),
	_ = require('lodash'),
	EventEmitter = require('events').EventEmitter,
	defaultExchangePublish = require('./default-exchange-publish');

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

var channelDict = {
	main: _createChannel(),
};

function Exchange(connString, params){
	var self = this;
	EventEmitter.call(self);
	params = _.extend({}, EXCHANGE_DEFAULTS, params);

	var delayAssertChannel;
	var confirmMode = !!params.confirm;
	delete params.confirm;

	var exchangeName = params.name || '';
	var getChannel;
	if (confirmMode) {
		getChannel = _createChannel(connString, true);
	} else if (connString) {
		getChannel = channelDict[connString];
	} else {
		getChannel = channelDict.main;
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
		get: function(){ return getChannel; }
	});

	this.publish = function(msg, publishOptions, cb){
		// make sure we arent publishing fancy mongoose objects
		if (msg && _.isFunction(msg.toObject)) {
			msg = msg.toObject();
		}

		return getChannel
			.then(function(chan){

				var options = _.extend({}, PUBLISH_DEFAULTS, publishOptions);
				var key = (options.key || 'blank').toString();

				delete options.key;

				msg._exchange = msg._exchange || exchangeName;

				if (confirmMode){
					chan.publish(exchangeName, key, Buffer(JSON.stringify(msg)), options);
					return chan.waitForConfirms()
						.then(function(){
							if (_.isFunction(cb)) cb();
							return true;
						});
				}

				return chan.publish(exchangeName, key, Buffer(JSON.stringify(msg)), options);
			});
	};

	this.delayedPublish = function(msg, publishOptions){
		// make sure we arent publishing fancy mongoose objects
		if (msg && _.isFunction(msg.toObject)) {
			msg = msg.toObject();
		}

		publishOptions = _.extend({}, DELAYED_PUBLISH_DEFAULTS, publishOptions);

		if (!delayAssertChannel) {
			delayAssertChannel = _createChannel(connString);
		}
		msg._exchange = msg._exchange || exchangeName;
		// negative delays break things
		var delay = Math.max(publishOptions.delay, 1);

		return delayAssertChannel
			.then(function(chan) {
				var queueName = 'delay_' + exchangeName  +'_by_'+publishOptions.delay+'__'+publishOptions.key;

				var options = {
					exclusive: false,
					autoDelete: false,
					arguments: {
						'x-dead-letter-exchange': exchangeName,
						'x-dead-letter-routing-key': publishOptions.key,
						'x-message-ttl': delay,
					},
				};

				return chan.assertQueue(queueName, options)
					.then(function() {
						console.log('xxxxx');
						return defaultExchangePublish(connString, msg, { key: queueName })
							.then(function() {
								return true;
							});
					});
			});
	};
}

util.inherits(Exchange, EventEmitter);

module.exports = function (opt = {}) {
	if (opt.connString && !channelDict[opt.connString]) {
		channelDict[opt.connString] = _createChannel(opt.connString);
	}

	return _.partial(Exchange, opt.connString);
};
