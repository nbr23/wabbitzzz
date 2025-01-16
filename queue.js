const uuid = require('ezuuid');
const _ = require('lodash');
const defaultExchangePublish = require('./default-exchange-publish');
const Promise = require('bluebird');
const getConnection = require('./get-connection');
const { deserialize } = require('./serializer.js');

const APP_NAME = process.env.APP_NAME || process.env.npm_package_name || 'UNKNOWN_APP';

var EXCHANGE_ATTRIBUTE_NAMES = [
	'durable',
	'autoDelete',
	'arguments',
	'internal',
	'alternateExchange',
];

var DEFAULTS = {
	exclusive: false,
	autoDelete: false,
	durable: true,
	ack: true,
	useErrorQueue: false,
};

function _log(level, ...args) {
	const fn = _.get(global.logger, level);

	if (fn) {
		fn(...args);
	} else {
		console.log(`[${level}] ${args}`);
	}
}

function getNoAckParam(params){
	if (params.noAck !== undefined && params.ack !== undefined){
		throw new Error('cannot specifiy both ack and noAck params');
	}

	if (params.noAck !== undefined) return !!params.noAck;
	if (params.ack !== undefined) return !params.ack;

	// default is to ack
	return false;
}

function _patternToMatcher(pattern) {
	if (!pattern) return _.constant(false);
	// https://github.com/mateodelnorte/amqp-match/blob/master/index.js
	const regexString = '^' +
		pattern.replace(/[.]/g, '\\.') // escape dots
			.replace(/\*/g, '([^.]+)') // single word wildcard
			.replace(/^#$/, '([^.]+[.]?)*') // multi word wildcard on its own
			.replace(/^#\\./, '([^.]+[.])*') // multi word wildcard at start of key
			.replace(/\\.#$/, '([.][^.]+)*') // multi word wildcard at end of key
			.replace(/\\.#\\./g, '(([.].*[.])*|[.])') // multi word wildcard within key
		+ '$';

	return function(str) {
		if (!str) return false;
		if (str === pattern) return true;

		return str.search(regexString) !== -1;
	};
}

function Queue(connString, params){
	params = _.extend({}, DEFAULTS, params);

	var name = (params.name || ((params.namePrefix || 'no_name') + '.no_mirror.' + uuid())).replace(/(\b|_)(APP_NAME)(\b|_)/, `$1${APP_NAME}$3`),
		useErrorQueue = !!params.useErrorQueue || _.isObject(params.errorQueue),
		errorQueueName = _.get(params, 'errorQueue.name', `${name}_error`),
		prefetchCount = params.prefetchCount|| 1,
		ctag,
		noAck = getNoAckParam(params),
		attempts = params.attempts,
		closing = false,
		exclusive = params.exclusive || false,
		durable = params.durable || false;

	const isQuorumQueue = _.get(params, 'arguments.x-queue-type') === 'quorum' || params.useQuorum;

	const consumerArgs = _.get(params, 'consumer.arguments', {});

	if (isQuorumQueue) {
		params.arguments = { ...params.arguments, ...{ 'x-queue-type': 'quorum' } };
	}

	if (exclusive) {
		params.durable = false;
	}

	if (noAck) {
		prefetchCount = 0;
	}

	var bindings = _.chain([params.exchangeNames])
		.concat([params.exchangeName])
		.concat([params.bindings])
		.concat([params.exchanges])
		.flatten()
		.filter(Boolean)
		.map(function(ex){
			if (_.isObject(ex)) return ex;
			if (_.isString(ex)) return  { name: ex };
			throw new Error('invalid binding/exchange');
		})
		.uniq(function(binding){ return [binding.name, binding.key].join('_'); })
		.forEach(function(binding){
			if (binding.key !== undefined) return;

			binding.key = binding.key || (params.key || '#').toString();
		})
		.value();

	const labeledBindings = bindings.filter(b => b.label && b.key);
	labeledBindings
		.forEach(b => {
			b.isMatch = _patternToMatcher(b.key);
		});

	delete params.exchangeName;
	delete params.exchangeNames;
	delete params.bindings;
	delete params.name;
	delete params.useErrorQueue;
	delete params.key;
	delete params.noAck;
	delete params.ack;
	delete params.exclusive;

	async function _createChannel() {
		const conn = await getConnection(connString)
		const chan = await conn.createChannel();

		chan.on('closed', function(){
			console.log('queue channel closed');
		});

		chan.on('error', function(err){
			console.log('queue channel errored', err?.message);
		});

		return chan;
	}

	let _restartConsumerAttempts = 0;
	async function _restartConsumer(fn) {
		ctag = null;

		_log('warn', `WABBITZZZ restart consumer on queue '${name}'.`);

		const connection = await getConnection(connString);
		const channel = await connection.createChannel();
		await channel.prefetch(prefetchCount);

		channel.on('error', function(err){
			console.log('_restartConsumer channel error', err.message);
		});

		try {
			if (!durable) {
				await _createQueue(channel, params);
			}

			const result = await _startConsumer(channel, fn, true);
			ctag = result.consumerTag;
			_log('success', `WABBITZZZ restart consumer on queue '${name}' succeeded in ${_restartConsumerAttempts} attempts`, ctag);
			_restartConsumerAttempts = 0;
		} catch (err) {
			_restartConsumerAttempts += 1;
			_log('warn', `WABBITZZZ restart consumer on queue '${name}' FAILED. Attempt #${_restartConsumerAttempts}.`);
		}

		if (ctag) {
			return true;
		}

		setTimeout(() => {
			_restartConsumer(fn);
		}, _.random(1000, 5000));

		return false;
	}

	async function _startConsumer(chan, fn, isRetry = false) {
		const consumerOptions = {
			exclusive,
			noAck,
			arguments: consumerArgs,
		};

		const consumeResult = await chan.consume(name, function(msg) {
			if (msg === null){
				if (closing) {
					return
				}

				// this means the queue has been cancelled. we should try re-consuming
				_log('warn', `WABBITZZZ consumer on queue '${name}' was cancelled by the server.`);

				ctag = null;

				if (_.isFunction(params.onConsumerCancelled)) {
					params.onConsumerCancelled();
				}

				return _restartConsumer(fn);
			}

			if (closing) {
				_log('warn', `channel for queue ${name} was closed before we could ACK` );
				return false;
			}
			var myMessage;
			try {

				myMessage = deserialize(msg);

				var retryDelay = 250;
				var maxAttempts;

				if (attempts) {
					myMessage._attempt = myMessage._attempt || 0;

					if (_.isArray(attempts)) {
						maxAttempts = _.size(attempts) + 1;
						retryDelay = attempts[myMessage._attempt];
					} else {
						maxAttempts = +attempts || 2;
					}

					if ((maxAttempts - 1) <= myMessage._attempt) {
						myMessage._isFinalAttempt = true;
					}
				}


				if (msg.properties){
					if (msg.properties.replyTo) myMessage._replyTo = msg.properties.replyTo;
					if (msg.properties.correlationId) myMessage._correlationId = msg.properties.correlationId;
				}

				if (msg.fields) {
					const { routingKey } = msg.fields;

					if (msg.fields.exchange) {
						myMessage._exchange = msg.fields.exchange;
						myMessage._routingKey = routingKey;
					}

					// there are potentially many matches, but we just use the first one. meh.
					const matchedBinding = _.find(labeledBindings, b => {
						// it is possible to apply the wrong label to a message if
						// we do not specify the exchange
						const isRightExchange = b.name === myMessage._exchange;
						if (!isRightExchange) return false;

						return b.isMatch && b.isMatch(routingKey);
					});
					if (matchedBinding) {
						myMessage._label = matchedBinding.label;
					}
				}
			} catch (err){
				_log('error', 'error deserializing message', err);
				myMessage = {};
			}

			var doneCalled = false;
			var done = function(error){
				if (noAck) return;

				doneCalled = true;

				if (!error) {
					return chan.ack(msg);
				}

				myMessage._error = _.extend({}, {message: error.message, stack: error.stack}, error);

				var pushToRetryQueue = false;

				try {
					if (attempts) {
						myMessage._attempt = myMessage._attempt || 0;

						myMessage._attempt += 1;

						if (myMessage._attempt < maxAttempts) {
							pushToRetryQueue = true;
						}
					}
				} catch (err) {
					_log('error', 'error while checking attempts', err);
				}

				if (pushToRetryQueue) {
					return defaultExchangePublish(connString, myMessage, { delay: retryDelay, key: name })
						.then(function(){
							return chan.ack(msg);
						});
				} else if (useErrorQueue) {
					var options = {
						key: errorQueueName,
						persistent: true,
					};

					return defaultExchangePublish(connString, myMessage, options)
						.then(function(){
							return chan.ack(msg);
						})
						.catch(function(publishError){
							_log('error', 'wabbitzzz, defaultExchangePublish error', error);
							_log('error', 'wabbitzzz, defaultExchangePublish publishError', publishError);

							throw publishError;
						});
				} else {
					_log('error', 'bad ack', error);
					return Promise.resolve(false);
				}
			};

			try {
				const myDone = (...args) => {
					// generous timeouts and then restart the whole thing.
					// TODO: try to reconnect instead of exiting
					return Promise.resolve(done.apply(null, args))
						.timeout(20000)
						.catch(err => {
							_log('error', 'our ack failed', err);

							return Promise.resolve(chan.nack(msg))
								.timeout(20000)
								.catch(err => {
									_log('our ack failed, then our nack failed', err);
									process.exit(1);
								});
						});
				};

				fn(myMessage, myDone);
			} catch (e){
				if (!doneCalled){
					done(e.toString());
				}
			}
		}, consumerOptions);

		if (isRetry) {
			_log('success', `WABBITZZZ consuming queue '${name}'.`, consumeResult.consumerTag);
		}

		return consumeResult;
	}

	function bindQueue(chan, bindings){
		return _.chain(bindings)
			.toArray()
			.map(function(binding){
				if (binding.type){
					var exParams = _.chain(EXCHANGE_ATTRIBUTE_NAMES)
						.filter(function(k){ return binding[k] !== undefined; })
						.map(function(k){ return [k, binding[k]]; })
						.fromPairs()
						.value();

					return chan.assertExchange(binding.name, binding.type, exParams)
						.then(function(){ return chan.bindQueue(name, binding.name, binding.key); });
				}

				return chan.bindQueue(name, binding.name, binding.key);
			})
			.thru(Promise.all)
			.value()
			.then(_.constant(chan));
	}

	async function _createQueue(chan, params, isRetry = false) {
		await chan.assertQueue(name, params);

		if (useErrorQueue){
			const errorOptions = {
				durable: true,
			};

			if (_.isObject(params.errorQueue)) {
				errorOptions.arguments = params.errorQueue.arguments;
			}

			if (isQuorumQueue) {
				errorOptions.arguments = { ...errorOptions.arguments, 'x-queue-type': 'quorum' };
			}

			await chan.assertQueue(errorQueueName, errorOptions)
		}

		await chan.prefetch(prefetchCount)
		await bindQueue(chan, bindings);

		if (_.isFunction(params.ready)) {
			params.ready();
		}

		if (isRetry) {
			_log('success', `WABBITZZZ created queue '${name}'.`);
		}
	}

	async function _ensureQueue(isRetry = false){
		const chan = await _createChannel();

		try {
			await _createQueue(chan, params, isRetry);
		} catch (err) {
			_log('error', 'unable to ensure queue', err.code, err);

			switch (err.code) {
				case 406: {
					_log('warn', `queue ${name} fatal 406. ending.`);
					return _die(err);
				}
				case 404: {
					_log('warn', `queue ${name} not found, retrying...`);
					await Promise.delay(_.random(1000, 5000));
					return _ensureQueue(true);
				}
				default: {
					throw err;
				}
			}
		}

		return chan;
	}

	async function _die(err){
		const connection = await getConnection(connString)
		await connection.close();
	}

	var queuePromise = _ensureQueue();

	var receiveFunc = function(fn){
		return queuePromise
			.then(function(chan){
				if (!chan) {
					_log('error', `missing channel for queue ${name} not consuming`);
					throw new Error(`missing channel for queue ${name} not consuming`);
				}

				if (closing) {
					_log('warn', `channel for queue ${name} was closed before we could start consuming`, 'HEY');
					return false;
				}

				return _startConsumer(chan, fn, false);
			})
			.then(function(res){
				ctag = res.consumerTag;
			})
			.catch(function(err){
				_log('error', 'there was a problem create queue: ' + name, err);
			});
	};

	receiveFunc.stop = function(){
		return queuePromise
			.then(function(chan){
				if (!ctag) return false;

				return chan.cancel(ctag);
			});
	};

	receiveFunc.close = function(){
		closing = true;

		return queuePromise
			.then(function(chan){
				return chan.close();
			});
	};

	receiveFunc.destroy = function(){
		closing = true;

		return queuePromise
			.then(async function(chan){
				try {
					if (ctag) {
						await chan.cancel(ctag);
					}

					await chan.deleteQueue(name)
					return chan;
				} catch (err) {
					_log(`error`, `unable to delete queue ${name}`, err);
					return chan;
				}
			})
			.then(function(chan){
				return chan.close();
			});
	};

	receiveFunc.addBinding = function(binding){
		return queuePromise
			.then(function(chan){
				if (closing) {
					return false;
				}

				if (binding.label && binding.key) {
					binding.isMatch = _patternToMatcher(binding.key);
					labeledBindings.push(binding);
				}

				return chan.bindQueue(name, binding.name, binding.key);
			});
	};

	receiveFunc.removeBinding = function(binding){
		return queuePromise
			.then(function(chan){
				return chan.unbindQueue(name, binding.name, binding.key);
			})
			.then(result => {
				if (binding.label) {
					_.pull(labeledBindings, b => b.label == binding.label);
				}

				return result;
			});
	};

	var property = Object.defineProperty.bind(Object, receiveFunc);
	property('ready', {
		get: function(){ return queuePromise; },
	});
	property('started', {
		get: function(){ return queuePromise; },
	});

	return receiveFunc;
}

module.exports = function (opt = {}) {
	return _.partial(Queue, opt.connString);
};
