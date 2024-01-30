const exchange = require('./exchange');
const _ = require('lodash');
const getConnection = require('./get-connection');
const { deserialize } = require('./serializer.js');

const DEFAULTS = {
	appName: '',
	ttl: 10000,
	shared: false,
};

function createOptions(methodName, options){
	switch (typeof methodName){
		case 'string':
			options = Object(options);
			options.methodName = methodName;
			break;
		case 'object':
			options = methodName;
	}

	options = _.extend({}, DEFAULTS, options);

	if (options.appName && !/_$/.test(options.appName))
		options.appName += '_';


	return options;
}
const MainExchange = exchange();
var defaultExchangeDict = {
	main: new MainExchange(),
};

function _createChannel(connString){
	return getConnection(connString)
		.then(async function(conn) {
			const chan = await conn.createChannel();

			chan.on('error', function(err){
				console.log('rpc channel error', err.message);
			});

			return chan;
		});
}

var channelDict = {};

function response (connString){
	var options = createOptions.apply(null, _.toArray(arguments).slice(1));
	const { appName = 'none', methodName } = options;
	const queueName = `${appName}_${methodName}___rpc`.replace(/-/g, '_'); // trailing _rpc important for policy regex

	let getChannel = channelDict[connString];

	if (!getChannel) {
		getChannel = channelDict[connString] = _createChannel(connString)
			.then(async function(chan){
				await chan.assertExchange('_rpc_send_direct', 'direct', {durable: true});
				await chan.prefetch(100);
				return chan;
			});
	}

	async function consume(chan, cb, isRetry) {
		if (!chan) {
			chan = await _createChannel(connString);
		}

		if (isRetry) {
			console.log(`resuming ${methodName} consumer.`, !!chan);
		}

		try {
			await chan.assertQueue(queueName, {
				durable: false,
				autoDelete: true,
				arguments: {
					'x-message-ttl': options.ttl,
				},
			});

			await chan.consume(queueName, (rawMsg) => {
				if (rawMsg === null) {
					console.log(`rpc consumer cancelled for ${methodName}. restarting...`);

					setTimeout(async () => {
						await consume(chan, cb, true);
					}, _.random(1000, 5000));

					return;
				}

				const contentType = _.get(rawMsg, 'properties.contentType', 'application/json');
				const done = function(err, res){
					const publishOptions = {
						persistent: false,
						key: rawMsg.properties.replyTo,
						correlationId: rawMsg.properties.correlationId,
						contentType,
					};

					const conn = connString ? connString : 'main';
					if (err){
						return defaultExchangeDict[conn].publish({
							_rpcError: true,
							_message: err.toString(),
						}, publishOptions);
					} else {
						return defaultExchangeDict[conn].publish(res, publishOptions);
					}
				};

				let msg;
				try {
					msg = deserialize(rawMsg);
				} catch (err) {
					msg = {};
					console.log(`deserialization error while processing ${methodName}`);
				}


				try {
					// this is not strictly necessary, but helps avoid bugs for the moment
					delete msg._exchange;
					cb(null, msg, done);
				} catch (err){
					console.log('unhandled error while processing ' + methodName);
					console.error(err);
					cb(err);
				}
			}, { noAck: true });

			await chan.bindQueue(queueName, '_rpc_send_direct', methodName);

			if (isRetry) {
				console.log(`RESUMED ${methodName} consumer`);
			}
		} catch (err) {
			setTimeout(() => {
				consume(null, cb);
			}, _.random(1000, 5000));
		}
	}

	return async (cb) => {
		const channel = await getChannel;
		await consume(channel, cb);
	};
}

module.exports = function (opt = {}) {
	if (opt.connString && !defaultExchangeDict[opt.connString]) {
		const AltExchange = exchange(opt);
		defaultExchangeDict[opt.connString] = new AltExchange();
	}
	return _.partial(response, opt.connString);
};
module.exports.createOptions = createOptions;
