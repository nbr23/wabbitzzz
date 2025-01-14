const _ = require('lodash');
const { version } = require('./package.json');
const CONN_STRING = process.env.WABBITZZZ_URL || 'amqp://localhost';
const amqplib = require('amqplib');
const Promise = require('bluebird');

process.on('unhandledRejection', (reason, p) => {
	// ECONNREFUSED is can be uncatchable somehow depending on node version ?!?!?
	const errorText = _.get(reason, 'message', '');
	const isECONNREFUSED = /ECONNREFUSED/.test(errorText);

	if (isECONNREFUSED) {
		const delay = _.random(2000, 5000);

		_log(`ECONNREFUSED EXITING FOUND. Exiting in ${delay}ms.`);
		setTimeout(function() {
			_log(`ECONNREFUSED EXITING NOW.`);
			process.exit(1);
		}, delay);
	} else {
		_log('Unhandled Rejection at: Promise', p, 'reason:', reason);
	}
});

const DEFAULT_CONNECTION_PARAMS = {
	clientProperties: {
		information: process.env.APP_NAME || process.env.npm_package_name,
		connection_name: process.env.APP_NAME || process.env.npm_package_name,
		product: `wabbitzzz v${version}`,
		version: '',
	},
};

function _log(...args) {
	if (global.logger && global.logger.warn) {
		global.logger.warn.apply(global.logger, ['WABBITZZZ', ...args]);
	} else {
		console.warn.apply(console, ['WABBITZZZ', ...args]);
	}
}

function _getConnection(connString = CONN_STRING){
	return Promise.resolve()
		.then(() => amqplib.connect(connString, DEFAULT_CONNECTION_PARAMS))
		.then(function(conn) {
			_log('CONNECTION OPENED');

			const cancelAllConsumers = async () => {
				_log('cancelAllConsumers called');
				const channels = conn.connection?.channels;
				if (!channels || channels.length === 0) {
					_log('no channels found when attempting to cancel all consumers');
					return;
				}

				const cancelConsumersResults = await Promise.resolve(channels)
					.mapSeries(async (channelObj) => {
						const channel = channelObj?.channel;
						if (channel?.consumers?.size > 0) {
							const consumerTags = Array.from(channel.consumers.keys());
							return Promise.resolve(consumerTags)
								.mapSeries(async (consumerTag) => {
									return channel.cancel(consumerTag)
										.then(() => ({succeeded: true, consumerTag }))
										.catch((err) => ({succeeded: false, consumerTag, reason: err}));
								});
						}

						return [];
					})
					.then((results) => results.flat())
					.catch((err) => {
						_log('error cancelling consumers', err);
						return [];
					});

				cancelConsumersResults.forEach(res => {
					if (!res.succeeded) {
						_log(`Error cancelling consumer: ${res.consumerTag} ${res.reason}`);
					} else {
						_log(`Cancelled consumer with consumer tag ${res.consumerTag}`);
					}
				});

				return cancelConsumersResults;
			};

			const stopConsumingAndClose = async () => {
				_log('signal received. stopping consuming before closing connection');
				await cancelAllConsumers();
				await new Promise(resolve => setTimeout(resolve, 10 * 1000));
				close();
			};

			var closed = false;
			function close(){
				if (closed){
					_log('close already ran');
					return;
				}
				_log('running close');
				closed = true;
				conn.close();
			}

			process.once('SIGTERM', stopConsumingAndClose);
			process.once('SIGINT', stopConsumingAndClose);
			conn.on('close', closeData => {
				_log('CONNECTION CLOSED', closeData);
				setTimeout(function() {
					_log(`connection closed EXITING NOW.`);
					process.exit(1);
				}, _.random(2000, 5000));
			});

			conn.on('error', err => {
				_log('CONNECTION ERRROR', err);
			});

			return conn;
		})
		.timeout(30000)
		.catch(function(err){
			_log(`unable to get connect: ${err.message}`);

			setTimeout(function() {
				_log(`unable to get connect EXITING NOW: ${err.message}`);
				process.exit(1);
			}, _.random(2000, 5000));

			throw err;
		});
}

var connectionsDict = {
	main: _getConnection(),
};
module.exports = function(connString){
	if (connString && !connectionsDict[connString]) {
		connectionsDict[connString] = _getConnection(connString);
	}

	return connString ? connectionsDict[connString] : connectionsDict.main;
};
