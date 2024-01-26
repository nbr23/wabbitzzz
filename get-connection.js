const _ = require('lodash');
const { version } = require('./package.json');
const CONN_STRING = process.env.WABBITZZZ_URL || 'amqp://localhost';
const amqplib = require('amqplib');
const Promise = require('bluebird');

// ECONNREFUSED is can be somehow uncatchable ?!?!?
process.on('unhandledRejection', (reason, p) => {
	console.error('wabbitzzz Unhandled Rejection at: Promise', p, 'reason:', reason);
	setTimeout(function() {
		_log(`unhandledRejection EXITING NOW.`);
		process.exit(1);
	}, _.random(2000, 5000));
});

const DEFAULT_CONNECTION_PARAMS = {
	clientProperties: {
		information: process.env.APP_NAME || process.env.npm_package_name,
		product: `wabbitzzz v${version}`,
		version: '',
	},
};

function _log(...args) {
	if (global.logger && global.logger.warn) {
		global.logger.warn.apply(global.logger, args);
	} else {
		console.warn.apply(console, args);
	}
}

function _getConnection(connString = CONN_STRING){
	return Promise.resolve()
		.then(() => amqplib.connect(connString, DEFAULT_CONNECTION_PARAMS))
		.then(function(conn) {
			_log('WABBITZZZ CONNECTION OPENED');

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

			process.once('SIGINT', close);
			conn.on('close', closeData => {
				_log('WABBITZZZ CONNECTION CLOSED', closeData);
				setTimeout(function() {
					_log(`connection closed EXITING NOW.`);
					process.exit(1);
				}, _.random(2000, 5000));
			});

			conn.on('error', err => {
				_log('WABBITZZZ CONNECTION ERRROR', err);
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
