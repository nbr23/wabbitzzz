const exchange = require('./exchange');
const _ = require('lodash');
const getConnection = require('./get-connection');

const DEFAULTS = {
	appName: '',
	ttl: 10000,
	shared: false,
};

function _parseJson(content){
	try {
		return JSON.parse(content.toString());
	} catch (e){
		console.error(`wabbitzzz rpc error parsing json:`, e);
		return {};
	}
}

function _deserializeMessage(rawMsg) {
	const { properties = {}, content } = rawMsg;
	let { contentType = 'application/json' } = properties;
	contentType = contentType.toLowerCase();

	switch (contentType) {
		case 'application/json': {
			return _parseJson(content);
		}
		default: {
			console.log(`wabbitzzz rpc unknown content type: ${contentType}. defaulting to json`);
			return _parseJson(content);
		}
	}
}



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
		.then(function(conn) {
			return conn.createChannel();
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
			})
			.catch(function(err){
				console.error(`wabbitzzz rpc setup error:`, err);
			});
	}
	
	getChannel
		.then(async chan => {
			const queueOptions = {
				durable: false,
				autoDelete: true,
				arguments: {
					'x-message-ttl': options.ttl,
				},
			};

			await chan.assertQueue(queueName, queueOptions);
		})
		.catch(function(err){
			console.error(`wabbitzzz rpc setup error:`, err);
		});

	return async (cb) =>{
		await getChannel
			.then(async function(chan){
				await chan.consume(queueName, (rawMsg) => {
					const done = function(err, res){
						const publishOptions = {
							persistent: false,
							key: rawMsg.properties.replyTo,
							correlationId: rawMsg.properties.correlationId,
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

					const msg = _deserializeMessage(rawMsg);
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
			})
			.catch(err => {
				console.error(err);
			});
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
