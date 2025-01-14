/* eslint-env mocha */
var ezuuid = require('ezuuid');
var expect = require('chai').expect;
const Queue = require('./queue')();
const Exchange = require('./exchange')();

describe('graceful shutdown', function() {
	it('should gracefully shutdown and stop receiving queued messages before exiting', async function(){
		this.timeout(20000);

		const messagesToPublish = [1, 2, 3, 4, 5, 6, 7, 8];
		const receivedPayloads = [];
		const queueName = ezuuid();
		const defaultExchange = new Exchange();
		let interval;

		defaultExchange.on('ready', async function(){
			var queue = new Queue({
				autoDelete: true,
				exclusive: true,
				name: queueName,
				ready: function() {
					interval = setInterval(() => {
						const payload = messagesToPublish.shift();
						if (!payload) {
							clearInterval(interval);
							return;
						}

						console.log('publishing msg with payload', payload);
						defaultExchange.publish({ payload }, { key: queueName })
							.then((res) => expect(res).to.be.true);
					}, 500);
				},
			});

			queue(function(msg, ack){
				receivedPayloads.push(msg.payload);
				if (msg.payload === 3) {
					process.emit('SIGINT');
				}
				ack();
			});
		});

		await new Promise((resolve) => {
			process.once('exit', (code) => {
				clearInterval(interval);
				expect(code).to.be.equal(1);
				expect(receivedPayloads).to.have.members([1,2,3]);
				expect(receivedPayloads).to.not.include.members([4,5,6,7,8]);
				resolve();
			});
		});
	});
});
