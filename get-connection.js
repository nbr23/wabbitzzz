var CONN_STRING = process.env.WABBITZZZ_URL || 'amqp://localhost';
var amqplib = require('amqplib');
var Promise = require('bluebird');
var path = require('path');
var projectName = path.basename(process.cwd());
var DEFAULT_CONNECTION_PARAMS = {
	clientProperties: {
		information: projectName,
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
	return Promise.resolve(amqplib.connect(connString, DEFAULT_CONNECTION_PARAMS))
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
				}, 5000);
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
			}, 5000);

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
