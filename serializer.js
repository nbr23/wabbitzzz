const { unpack, pack } = require('msgpackr');


function deserialize(msg){
	const { contentType = 'application/json' } = msg.properties;

	switch (contentType) {
		case 'application/msgpack': {
			return unpack(msg.content);
		}

		default:
		case 'application/json': {
			return JSON.parse(msg.content);
		}
	}
}

function serialize(obj, contentType = 'application/json'){
	switch (contentType) {
		case 'application/msgpack': {
			return pack(obj);
		}

		case 'application/json': {
			return Buffer(JSON.stringify(obj));
		}

		default: {
			throw new Error(`Unknown content type: ${contentType}`);
		}
	}
}

module.exports = {
	deserialize,
	serialize,
};
