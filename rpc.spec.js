/* eslint-env mocha */

const  request = require('./request')();
const Promise  = require('bluebird');
const _  = require('lodash');
const response = require('./response')();
const ezuuid = require('ezuuid');
const { expect } = require('chai');

describe('rpc', function(){
	it('should be able to make rpc calls', async function(){
		this.timeout(40000);

		const METHOD_NAME = ezuuid();
		const listen = response(METHOD_NAME);
		const key = ezuuid();

		await listen(function(err, req, cb){
			cb(null, { isResponse: true, msg: `${req.msg}_${key}` });
		});

		await Promise.resolve(_.range(6))
			.mapSeries(async i => {
				return new Promise(function(resolve, reject){
					request(METHOD_NAME)({msg: i}, function(err, res){
						if (err) return reject(err);

						const expected = `${i}_${key}`;
						expect(res.msg).to.be.equal(expected);
						resolve();
					});
				});
			});

		return true;
	});

	it('should handle timeouts', async function(){
		const METHOD_NAME = ezuuid();
		const listen = response(METHOD_NAME);
		let gotIt = false;


		const d = Promise.defer();
		await listen(function(){
			gotIt = true;
		});

		request(METHOD_NAME, { timeout: 100 })({msg: 'goodbye cruel world'}, (err) => {
			if (err && gotIt) d.resolve(true);
		});

		return d.promise;
	});

	it('should handle trees in the forest type of thing', function(done){
		const METHOD_NAME = 'beaches_do_not_exist';

		request(METHOD_NAME, {timeout: 100})({msg: 'goodbye cruel world'}, function(err){
			expect(err).to.be.ok;
			expect(err.message).to.be.equal(`timeout on ${METHOD_NAME}`);

			if (err) return done();

			done(new Error('there was no error is the error'));
		});
	});

	it('should expire messages properly', async function(){
		const METHOD_NAME = ezuuid();
		const d = Promise.defer();
		const listen = response({methodName: METHOD_NAME, ttl: 100});

		request({ methodName: METHOD_NAME, timeout: 300 })({ code: 'a' }, function(err){
			if (!err) {
				return d.reject(new Error('we expected a timeout error'));
			}
		});

		await Promise.delay(400);

		await listen(function(err, req){
			if (err) {
				return d.reject(err);
			} else if (req) {
				if (req.code === 'a') {
					d.reject(new Error('this message should have expired'));
				} else if (req.code === 'b') {
					d.resolve(true);
				} else {
					d.reject(new Error('unknown error'));
				}
			} else {
				d.reject(new Error('no error and no request'));
			}
		});

		request({ methodName: METHOD_NAME, timeout: 500 })({ code: 'b' }, _.noop);

		return d.promise;
	});

	it('should not expire delayed messages if within timeout', async function(){
		this.timeout(10000);

		const METHOD_NAME= 'this_is_my_other_ttl_test';
		const listen = response({methodName: METHOD_NAME, ttl: 3000});

		await listen(function(err, req, cb){
			cb(null, {message: 'hello2'});
		});

		const d = Promise.defer();
		request({methodName: METHOD_NAME, timeout: 4000})({code: 'a'}, function(err, res){
			if (err) return d.reject(err);

			if (res.message === 'hello2') {
				d.resolve(true);
			}
		});

		return d.promise;

	});

	it('should be able to round robin requests', async function(){
		const d = Promise.defer();
		const METHOD_NAME = ezuuid();
		const listen1 = response({ methodName: METHOD_NAME, ttl: 3000 });
		const listen2 = response({ methodName: METHOD_NAME, ttl: 3000 });
		const listen3 = response({ methodName: METHOD_NAME, ttl: 3000 });

		await listen1(function(err, req, cb){
			if (heard1) return d.reject(new Error('already heard1'));
			heard1 = true;
			cb(null, {message: 'hello1'});
			allDone();
		});

		await listen2(function(err, req, cb){
			if (heard2) return d.reject(new Error('already heard2'));
			heard2 = true;
			cb(null, {message: 'hello2'});
			allDone();
		});

		await listen3(function(err, req, cb){
			if (heard3) return d.reject(new Error('already heard3'));
			heard3 = true;
			cb(null, {message: 'hello3'});
			allDone();
		});

		request({appName: 'shared_test_', methodName: METHOD_NAME, timeout: 4000})({code: 'a'}, function(err){
			if (err) return d.reject(err);
		});

		request({appName: 'shared_test_', methodName: METHOD_NAME, timeout: 4000})({code: 'b'}, function(err){
			if (err) return d.reject(err);
		});

		request({appName: 'shared_test_', methodName: METHOD_NAME, timeout: 4000})({code: 'c'}, function(err){
			if (err) return d.reject(err);
		});

		var heard1, heard2, heard3, isDone;
		function allDone(){
			if (isDone) return;

			if (heard1 && heard2 && heard3) {
				isDone = true;
				d.resolve(true);
			}
		}

		return d.promise;
	});

	it('should return errors properly', async function(){
		this.timeout(5000);

		const d = Promise.defer();
		const METHOD_NAME = ezuuid();
		const listen = response({methodName: METHOD_NAME, ttl: 3000, shared: true});

		await listen(function(err, req, cb){
			cb(new Error('this is an error'));
		});

		request({ methodName: METHOD_NAME, timeout: 4000})({code: 'xxx'}, function(err){
			if (err) {
				d.resolve(true);
			}
		});

		return d.promise;
	});
});
