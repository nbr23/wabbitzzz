const _ = require('lodash');
const getConnection = require('./get-connection');

const PUBLISH_DEFAULTS = {
	persistent: false,
	contentType: 'application/json',
};
const { serialize } = require('./serializer.js');

var channelDict = {
	main: getChannel(),
};

function getChannel (connString) {
	return getConnection(connString)
		.then(function(conn){
			return conn.createConfirmChannel();
		});
}

function _publish(connString, msg, options){
	const conn = connString ? connString : 'main';
	if (!channelDict[conn]) {
		channelDict[conn] = getChannel(connString);
	}
	return channelDict[conn]
		.then(function(chan){
			var key = options.key;
			var delay = options.delay;

			options = _.extend({}, PUBLISH_DEFAULTS, options);
			delete options.key;
			delete options.delay;

			const buf = serialize(msg, options.contentType);
			if (delay) {
				const queueOptions = {
					arguments: {
						'x-dead-letter-exchange': '',
						'x-dead-letter-routing-key': key,
						'x-message-ttl': delay,
					},
				};

				const delayQueueName = `delay_default_${key}_${delay}`;
				return chan.assertQueue(delayQueueName, queueOptions)
					.then(() => {
						chan.publish('', delayQueueName, buf, options);
						return chan.waitForConfirms();
					});
			} else {

				chan.publish('', key, buf, options);
				return chan.waitForConfirms();
			}

		})
		.timeout(20 * 1000);
}

module.exports = _publish;
