const EventEmitter = require('events');
class PubSub extends EventEmitter {}

const pubSub = new PubSub();
module.exports = pubSub;