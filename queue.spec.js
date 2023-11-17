/* eslint-env mocha */

var Queue = require('./queue')(),
	Promise = require('bluebird'),
	_ = require('lodash'),
	Exchange = require('./exchange')(),
	ezuuid = require('ezuuid'),
	expect = require('chai').expect;

describe('queue', function(){
	it('should be able to bind to a single exchange', function(done){
		this.timeout(5000);

		var exchangeName = ezuuid();
		var message = ezuuid();

		var exchange1 = new Exchange({name: exchangeName, autoDelete: true});

		exchange1.on('ready', function(){

			var queue = new Queue({
				autoDelete: true,
				exclusive: true,
				exchangeName: exchangeName,
				ready: function(){
					exchange1.publish({key:message});
				},
			});

			queue(function(msg, ack){
				if (msg.key !== message) return done('got a message I shouldnt have');

				ack();
				done();
			});

		});
	});
	it('should be able to recieve messagepack messages', async function(){
		const d = Promise.defer();
		const exchangeName = ezuuid();

		const exchange1 = new Exchange({
			name: exchangeName,
			type: 'direct',
		});

		const queue = new Queue({
			autoDelete: true,
			name: `queue_${exchangeName}`,
			bindings: [
				{ name: exchangeName, type: 'direct', key: 'what' },
			],
		});

		await queue((msg, ack) => {
			expect(msg.message).to.eql('hey');
			d.resolve(true);
			ack();
		});

		await exchange1.publish({ message: 'hey' }, { key: 'what', contentType: 'application/json' });

		return d.promise;
	});

	it('should be able to bind to a single exchange with the exchangeNames property', function(done){
		this.timeout(5000);

		var exchangeName = ezuuid();
		var message = ezuuid();

		var exchange1 = new Exchange({name: exchangeName, autoDelete: true});

		exchange1.on('ready', function(){

			var queue = new Queue({
				autoDelete: true,
				exclusive: true,
				exchangeNames: [exchangeName],
				ready: function(){
					exchange1.publish({key:message});
				},
			});

			queue(function(msg, ack){
				if (msg.key !== message) return done('got a message I shouldnt have');

				ack();
				done();
			});

		});
	});

	it('should be able to bind to multiple exchanges', function(done){
		this.timeout(5000);

		var exchangeName1 = ezuuid(),
			exchangeName2 = ezuuid(),
			message1 = ezuuid(),
			message2 = ezuuid();

		var exchange1 = new Exchange({name: exchangeName1, autoDelete: true, durable: false});
		var exchange2 = new Exchange({name: exchangeName2, autoDelete: true, durable: false});

		Promise.all([exchange1.ready, exchange2.ready])
			.then(function(){
				var queue = new Queue({
					autoDelete: true,
					exclusive: true,
					exchangeNames: [exchangeName1, exchangeName2],
				});

				var gotMessage1 = false,
					gotMessage2 = false;

				queue(function(msg, ack){
					if (msg.key === message1) {
						gotMessage1 = true;
					}
					if (msg.key === message2) {
						gotMessage2 = true;
					}

					ack();

					if (gotMessage1 && gotMessage2){
						done();
					}
				});

				_.delay(exchange1.publish, 200, {key:message1});
				_.delay(exchange2.publish, 200, {key:message2});
			});
	});

	it('should include the _exchange property', function(done){
		this.timeout(5000);

		var exchangeName = ezuuid();
		var message = ezuuid();

		var exchange1 = new Exchange({name: exchangeName, autoDelete: true});

		exchange1.on('ready', function(){

			var queue = new Queue({
				autoDelete: true,
				exclusive: true,
				exchangeName: exchangeName,
				ready: function(){
					exchange1.publish({key:message});
				},
			});

			queue(function(msg, ack){
				if (msg.key !== message) return done('got a message I shouldnt have');

				if (msg._exchange !== exchangeName) return done('bad _exchangeName');

				ack();
				done();
			});

		});
	});

	it('should be able to push errors to xxx_error queue', function(done){
		this.timeout(8000);

		function _readError(){
			var errorQueue = new Queue({
				name: queueName +  '_error',
			});

			errorQueue(async function(msg, ack){
				expect(msg._error).to.be.ok;
				expect(msg._error.message).to.be.equal(errorKey);
				await ack();

				errorQueue.destroy();
				done();
			});

		}

		var exchangeName = ezuuid();
		var queueName = ezuuid();
		var message = ezuuid();
		var errorKey = ezuuid();

		var exchange1 = new Exchange({name: exchangeName, autoDelete: true});

		exchange1.on('ready', function(){
			var queue = new Queue({
				name: queueName,
				autoDelete: true,
				exclusive: true,
				useErrorQueue: true,
				exchangeName: exchangeName,
				ready: function(){
					exchange1.publish({ key: message });
				},
			});

			queue(async function(msg, ack){
				if (msg.key !== message) return done('got a message I shouldnt have');
				if (msg._exchange !== exchangeName) return done('bad _exchangeName');

				await ack(new Error(errorKey));

				queue.destroy();
				_readError();
			});
		});
	});
	it('should be able to push errors to custom error queue', function(done){
		this.timeout(8000);
		const errorQueueName = ezuuid();

		function _readError(){
			var errorQueue = new Queue({
				name: errorQueueName,
			});

			errorQueue(async function(msg, ack){
				expect(msg._error).to.be.ok;
				expect(msg._error.message).to.be.equal(errorKey);
				await ack();

				errorQueue.destroy();
				done();
			});

		}

		var exchangeName = ezuuid();
		var queueName = ezuuid();
		var message = ezuuid();
		var errorKey = ezuuid();

		var exchange1 = new Exchange({name: exchangeName, autoDelete: true});

		exchange1.on('ready', function(){
			var queue = new Queue({
				name: queueName,
				autoDelete: true,
				exclusive: true,
				errorQueue: {
					name: errorQueueName,
				},
				exchangeName: exchangeName,
				ready: function(){
					exchange1.publish({ key: message });
				},
			});

			queue(async function(msg, ack){
				if (msg.key !== message) return done('got a message I shouldnt have');
				if (msg._exchange !== exchangeName) return done('bad _exchangeName');

				await ack(new Error(errorKey));

				queue.destroy();
				_readError();
			});
		});
	});

	it('should be able to bind with different routing keys', function(done){
		this.timeout(5000);

		var exchangeName1 = ezuuid(),
			exchangeName2 = ezuuid(),
			message1 = ezuuid(),
			message2 = ezuuid();

		var key1 = 'some_cool_key';
		var key2 = 'some_lame_key';
		var exchange1 = new Exchange({name: exchangeName1, autoDelete: true, durable: false, type: 'direct'});
		var exchange2 = new Exchange({name: exchangeName2, autoDelete: true, durable: false, type: 'direct'});

		Promise.all([exchange1.ready, exchange2.ready])
			.then(function(){
				var queue = new Queue({
					autoDelete: true,
					exclusive: true,
					exchanges: [
						{ name: exchangeName1, key: key1 },
						{ name: exchangeName2, key: key2 },
					],
				});

				var gotMessage1 = false,
					gotMessage2 = false;

				queue(function(msg, ack){
					if (msg.msg === message1) {
						gotMessage1 = true;
					} else if (msg.msg === message2) {
						gotMessage2 = true;
					} else {
						done(new Error('got a bad message'));
					}

					ack();

					if (gotMessage1 && gotMessage2){
						done();
					}
				});

				_.delay(exchange1.publish, 200, {msg:message1}, {key: 'some_wrong_key' });
				_.delay(exchange2.publish, 200, {msg:message2}, {key: 'some_wrong_key' });
				_.delay(exchange1.publish, 400, {msg:message1}, {key: key1 });
				_.delay(exchange2.publish, 400, {msg:message2}, {key: key2 });
			});
	});

	it('should be able to create an exchange on the fly if give the data', function(done){
		var exchangeName1 = ezuuid(),
			exchangeName2 = ezuuid(),
			message1 = ezuuid(),
			message2 = ezuuid();

		var key1 = 'some_cool_key';
		var key2 = 'some_lame_key';

		var queue = new Queue({
			autoDelete: true,
			exclusive: true,
			bindings: [
				{ name: exchangeName1, key: key1, type: 'direct', durable: false, autoDelete: true },
				{ name: exchangeName2, key: key2, type: 'direct', durable: false, autoDelete: true },
			],
			ready: function(){
				// now that the binding should have created the exchanges already
				var exchange1 = new Exchange({name: exchangeName1, autoDelete: true, durable: false, type: 'direct'});
				var exchange2 = new Exchange({name: exchangeName2, autoDelete: true, durable: false, type: 'direct'});

				_.delay(exchange1.publish, 200, {msg:message1}, {key: 'some_wrong_key' });
				_.delay(exchange2.publish, 200, {msg:message2}, {key: 'some_wrong_key' });
				_.delay(exchange1.publish, 400, {msg:message1}, {key: key1 });
				_.delay(exchange2.publish, 400, {msg:message2}, {key: key2 });
			},
		});

		var gotMessage1 = false,
			gotMessage2 = false;

		queue(function(msg, ack){
			if (msg.msg === message1) {
				gotMessage1 = true;
			} else if (msg.msg === message2) {
				gotMessage2 = true;
			} else {
				done(new Error('got a bad message'));
			}

			ack();

			if (gotMessage1 && gotMessage2){
				done();
			}
		});
	});

	it('should be able to add bindings on the fly', function(done){
		var content = { now: Date.now() };
		var content2 = { now: Date.now() + '_hello1' };

		var exchangeName = 'my_bootleg_exchange';
		var ex = new Exchange({ name: exchangeName, type: 'topic' });

		var coolExchangeName = 'my_cool_exchange';
		var coolExchange = new Exchange({ name: coolExchangeName, type: 'topic' });

		var queue = new Queue({
			autoDelete: true,
			exclusive: true,
			bindings: [ { name: exchangeName, type: 'topic', key: '#' } ],
			ready: function(){
				ex.publish(content);
			},
		});

		var messageCount = 0;
		queue(function(msg, ack){
			if (messageCount > 0){
				expect(msg.now).to.be.equal(content2.now);
				expect(msg._routingKey).to.be.equal('1111');
				ack();
				done();
			}

			messageCount++;

			expect(msg.now).to.be.equal(content.now);

			queue.addBinding({ name: coolExchangeName, key: '1111' })
				.then(() => coolExchange.publish(content2, { key: '1111' }))
				.then(() => ack());
		});
	});

	it('should be able to remove bindings on the fly', function(done){
		var content1 = { now: Date.now() };
		var content2 = { now: Date.now() + '_hello2' };
		var content3 = { now: Date.now() + '_yah_yah' };

		var exchangeName1 = 'my_bootleg_exchange1';
		var exchange1 = new Exchange({ name: exchangeName1, type: 'topic', confirm: true });

		var exchangeName2 = 'my_cool_exchange2';
		var exchange2 = new Exchange({ name: exchangeName2, type: 'topic', confirm: true });

		var queue = new Queue({
			autoDelete: true,
			exclusive: true,
			bindings: [
				{ name: exchangeName1, type: 'topic', key: '#' },
				{ name: exchangeName2, type: 'topic', key: '#' },
			],
			ready: function(){
				exchange1.publish(content1)
					.then(() => queue.removeBinding({ name: exchangeName1, key: '#' }))
					.then(() => exchange1.publish(content2))
					.then(() => exchange2.publish(content3));
			},
		});

		var messageCount = 0;
		queue(function(msg, ack){
			if (messageCount === 0){
				expect(msg.now).to.be.equal(content1.now);
				ack();

				return messageCount++;
			}

			if (messageCount === 1){
				expect(msg.now).to.be.equal(content3.now);
				ack();
				done();

				return messageCount++;
			}

			expect(true).to.be.false;
		});
	});

	describe('retries', function() {
		it('should retry messages when attempts is set', function(done){
			this.timeout(25000);
			var content1 = { id: ezuuid() };

			var exchangeName1 = 'my_exchange_to_retry';
			var exchange1 = new Exchange({ name: exchangeName1, type: 'topic', confirm: true });

			var queue = new Queue({
				autoDelete: true,
				exclusive: true,
				attempts: 3,
				bindings: [
					{ name: exchangeName1, type: 'topic', key: '#' },
				],
				ready: function(){
					exchange1.publish(content1);
				},
			});

			var myAttemptCount = 0;

			queue(function(msg, ack){
				try {
					expect(msg.id).to.be.equal(content1.id);
					if (myAttemptCount === 0) {
						expect(msg._attempt).to.be.not.ok;
						myAttemptCount++;
					} else {
						expect(myAttemptCount++).to.be.equal(msg._attempt);
					}


					if (msg._attempt === 2) {
						// we did it!
						ack();
						return done();
					}

					ack('retry');
				} catch (err) {
					done(err);
				}
			});
		});

		it('should not retry messages more than the # of attempts', function(done) {
			this.timeout(25000);
			var content1 = { id: ezuuid() };

			var queueName = ezuuid();
			var exchangeName1 = 'my_exchange_to_retry3';
			var exchange1 = new Exchange({ name: exchangeName1, type: 'topic', confirm: true });

			var errorQueue = new Queue({
				name: queueName + '_error',
				durable: true,
			});

			var maxAttempts = 3;
			var queue = new Queue({
				name: queueName,
				useErrorQueue: true,
				autoDelete: true,
				exclusive: true,
				attempts: maxAttempts,
				bindings: [
					{ name: exchangeName1, type: 'topic', key: '#' },
				],
				ready: function() {
					exchange1.publish(content1);
				},
			});

			var myAttemptCount = 0;

			queue(function(msg, ack) {
				try {
					expect(msg.id).to.be.equal(content1.id);

					if (myAttemptCount === 0) {
						expect(msg._attempt).to.be.not.ok;
					} else {
						expect(myAttemptCount).to.be.equal(msg._attempt);
					}

					myAttemptCount++;

					expect(myAttemptCount).to.be.lte(maxAttempts);

					if (myAttemptCount === maxAttempts) {
						expect(msg._isFinalAttempt).to.be.true;
					} else {
						expect(msg._isFinalAttempt).to.be.not.ok;
					}

					ack('retry');
				} catch (err) {
					done(err);
				}
			});

			errorQueue(function(msg, ack) {
				expect(msg.id).to.be.equal(content1.id);
				expect(myAttemptCount).to.be.equal(maxAttempts);

				ack();
				done();
			});
		});

		it('should retry messages when attempts is an array', function(done) {
			this.timeout(25000);
			var content1 = { id: ezuuid() };

			var queueName = ezuuid();
			var exchangeName1 = 'my_exchange_to_retry2';
			var exchange1 = new Exchange({ name: exchangeName1, type: 'topic', confirm: true });

			var errorQueue = new Queue({
				name: queueName + '_error',
				durable: true,
			});

			var maxAttempts = [1000, 5000, 2000];
			var maxAttemptCount = _.size(maxAttempts) + 1;

			var queue = new Queue({
				name: queueName,
				useErrorQueue: true,
				autoDelete: true,
				exclusive: true,
				attempts: maxAttempts,
				bindings: [
					{ name: exchangeName1, type: 'topic', key: '#' },
				],
				ready: function() {
					exchange1.publish(content1);
				},
			});

			var myAttemptCount = 0;

			var now = Date.now();
			queue(function(msg, ack) {
				try {
					var n = Date.now();
					expect(msg.id).to.be.equal(content1.id);

					if (myAttemptCount === 0) {
						expect(msg._attempt).to.be.not.ok;
					} else {
						expect(myAttemptCount).to.be.equal(msg._attempt);
					}

					myAttemptCount++;

					expect(myAttemptCount).to.be.lte(maxAttemptCount);

					if (myAttemptCount === maxAttemptCount) {
						expect(msg._isFinalAttempt).to.be.true;
					} else {
						expect(msg._isFinalAttempt).to.be.not.ok;
					}

					ack('retry');
				} catch (err) {
					done(err);
				}
			});

			errorQueue(function(msg, ack) {
				expect(msg.id).to.be.equal(content1.id);
				expect(myAttemptCount).to.be.equal(_.size(maxAttempts) + 1);

				ack();
				done();
			});
		});
	});
	it('should work with fancy labels', function(done){
		this.timeout(5000);

		var exchangeName = ezuuid();
		var message = ezuuid();

		var exchange1 = new Exchange({name: exchangeName, type: 'topic',  autoDelete: true});

		exchange1.on('ready', function(){

			var queue = new Queue({
				autoDelete: true,
				exclusive: true,
				bindings: [
					{ name: exchangeName, key: 'mike.*', label: 'one' },
					{ name: exchangeName, key: 'fred.*' , label: 'two' },
				],
				ready: function(){
					exchange1.publish({key:message}, { key: 'fred.888what' });
				},
			});

			queue(function(msg, ack){
				if (msg.key !== message) return done('got a message I shouldnt have');

				expect(msg._label).to.be.equal('two');
				ack();
				done();
			});

		});

	});
	it('should not apply the wrong label', function (done) {
		this.timeout(5000);

		const exchange1Name = ezuuid();
		const exchange2Name = ezuuid();
		
		const exchange1 = new Exchange({
			name: exchange1Name,
			type: 'topic',
		});

		const exchange2 = new Exchange({
			name: exchange2Name,
			type: 'fanout',
		});

		const queue = new Queue({
			autoDelete: true,
			exclusive: true,
			bindings: [
				{
					name: exchange1Name,
					type: 'topic',
					key: '#',
					label: 'freaky_deaky',
				},
				{
					name: exchange2Name,
					type: 'fanout',
				},
			],
			ready: function () {
				exchange2.publish({ bananas: true });
			},
		});

		exchange1.on('ready', function () {
			exchange2.on('ready', function () {

				queue(function (msg, ack) {
					if (msg._exchange !== exchange2Name) {
						return done('something is messed up');
					}

					expect(msg._label).to.not.be.ok;
					ack();
					done();
				});

			});
		});
	});
});
