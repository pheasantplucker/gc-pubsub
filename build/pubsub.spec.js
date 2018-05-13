'use strict';

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

const {
  assertSuccess,
  assertFailure,
  payload
} = require(`@pheasantplucker/failables-node6`);
const {
  createPublisher,
  createSubscriber,
  createTopic,
  topicExists,
  deleteTopic,
  createSubscription,
  deleteSubscription,
  subscriptionExists,
  publish,
  publishJson,
  publishMany,
  publishManyJson,
  pull,
  acknowledge
} = require('./pubsub');
const uuid = require('uuid');
const equal = require('assert').deepEqual;

const { GC_PROJECT_ID } = process.env;

describe(`pubsub.js`, function () {
  const _createPublisher = (config = {}) => {
    const result = createPublisher(GC_PROJECT_ID);
    assertSuccess(result);
  };
  const _createSubscriber = (config = {}) => {
    const result = createSubscriber(GC_PROJECT_ID);
    assertSuccess(result);
  };

  describe(`createPublisher()`, () => {
    it(`should create a publisher`, () => {
      _createPublisher();
    });
  });

  describe(`createTopic() & topicExists() & deleteTopic()`, () => {
    const topicName = `lib_test_${uuid.v4()}`;

    _createPublisher();

    it(`should create the topic`, _asyncToGenerator(function* () {
      const result = yield createTopic(topicName);
      assertSuccess(result);
    }));

    it(`should exist`, _asyncToGenerator(function* () {
      const result = yield topicExists(topicName);
      assertSuccess(result, true);
    }));

    it(`should delete the topic`, _asyncToGenerator(function* () {
      const result = yield deleteTopic(topicName);
      assertSuccess(result, topicName);
    }));

    it(`should not exist anymore`, _asyncToGenerator(function* () {
      const result = yield topicExists(topicName);
      assertSuccess(result, false);
    }));

    it(`should fail with no topic name`, _asyncToGenerator(function* () {
      const result = yield createTopic();
      assertFailure(result);
    }));
  });

  describe(`createSubscription() & subscriptionExists() & deleteSubscription()`, () => {
    const topicName = `lib_test_topic_${uuid.v4()}`;
    const subscriptionName = `lib_test_sub_${uuid.v4()}`;
    it(`should create publisher`, _asyncToGenerator(function* () {
      _createPublisher();
    }));
    it(`should create subscriber`, _asyncToGenerator(function* () {
      _createSubscriber();
    }));

    it(`should fail without a topic`, _asyncToGenerator(function* () {
      const result = yield createSubscription('', subscriptionName);
      assertFailure(result);
    }));

    it(`should create a topic`, _asyncToGenerator(function* () {
      const result = yield createTopic(topicName);
      assertSuccess(result);
    }));

    it(`should create the subscription`, _asyncToGenerator(function* () {
      const result = yield createSubscription(topicName, subscriptionName);
      assertSuccess(result);
    }));

    it(`subscription should exist`, _asyncToGenerator(function* () {
      const result = yield subscriptionExists(subscriptionName);
      assertSuccess(result, true);
    }));

    it(`should delete the topic`, _asyncToGenerator(function* () {
      const result = yield deleteTopic(topicName);
      assertSuccess(result, topicName);
    }));

    it(`should delete the subscription`, _asyncToGenerator(function* () {
      const result = yield deleteSubscription(subscriptionName);
      assertSuccess(result, subscriptionName);
    }));

    it(`subscription should not exist`, _asyncToGenerator(function* () {
      const result = yield subscriptionExists(subscriptionName);
      assertSuccess(result, false);
    }));
  });

  describe('publish() & pull() & acknowledge()', () => {
    const topicName = `lib_test_${uuid.v4()}`;
    const subscriptionName = `lib_test_${uuid.v4()}`;

    it(`create subscriber`, () => {
      _createSubscriber();
    });

    it(`create publisher`, () => {
      _createPublisher();
    });

    it(`should create a topic`, _asyncToGenerator(function* () {
      const result = yield createTopic(topicName);
      assertSuccess(result);
    }));

    it(`should create the subscription`, _asyncToGenerator(function* () {
      const result = yield createSubscription(topicName, subscriptionName);
      assertSuccess(result);
    }));

    it(`should publish an object message`, _asyncToGenerator(function* () {
      const message = {
        data: 'la la la I am a string',
        attributes: { bikes: 'cool' }
      };
      const result = yield publish(topicName, message);
      assertSuccess(result);
    }));

    let ackId;
    it(`should pull message`, _asyncToGenerator(function* () {
      const maxMessages = 1;
      const returnImmediately = true;
      const result = yield pull(subscriptionName, maxMessages, returnImmediately);
      ackId = payload(result)[0].receivedMessages[0].ackId;
      assertSuccess(result);
    }));

    it(`should acknowledge the message`, _asyncToGenerator(function* () {
      const messageIds = [ackId];
      const result = yield acknowledge(subscriptionName, messageIds);
      assertSuccess(result);
    }));

    it(`should have no messages`, _asyncToGenerator(function* () {
      const maxMessages = 1;
      const returnImmediately = true;
      const result = yield pull(subscriptionName, maxMessages, returnImmediately);
      const response = payload(result);
      const { receivedMessages } = response[0];
      assertSuccess(result);
      equal(receivedMessages.length, 0);
    }));

    it(`delete the topic`, _asyncToGenerator(function* () {
      const result = yield deleteTopic(topicName);
      assertSuccess(result);
    }));

    it(`should delete the subscription`, _asyncToGenerator(function* () {
      const result = yield deleteSubscription(subscriptionName);
      assertSuccess(result, subscriptionName);
    }));
  });

  describe(`publishMany()`, () => {
    const topicName = `lib_test_${uuid.v4()}`;
    const subscriptionName = `lib_test_${uuid.v4()}`;

    it(`create subscriber`, () => {
      _createSubscriber();
    });
    it(`create publisher`, () => {
      _createPublisher();
    });

    it(`should create a topic`, _asyncToGenerator(function* () {
      const result = yield createTopic(topicName);
      assertSuccess(result);
    }));

    it(`should create the subscription`, _asyncToGenerator(function* () {
      const result = yield createSubscription(topicName, subscriptionName);
      assertSuccess(result);
    }));

    it(`should publish many messages`, _asyncToGenerator(function* () {
      const message1 = {
        data: 'bleep blop bloop',
        attributes: { today: 'friday' }
      };
      const message2 = {
        data: 'hazah hazah hazah',
        attributes: { today: 'saturday' }
      };

      const result = yield publishMany(topicName, [message1, message2]);
      assertSuccess(result, 2);
    }));

    it(`should pull message`, _asyncToGenerator(function* () {
      const maxMessages = 2;
      const returnImmediately = true;
      const result = yield pull(subscriptionName, maxMessages, returnImmediately);
      assertSuccess(result);
    }));

    it(`delete the topic`, _asyncToGenerator(function* () {
      const result = yield deleteTopic(topicName);
      assertSuccess(result);
    }));

    it(`should delete the subscription`, _asyncToGenerator(function* () {
      const result = yield deleteSubscription(subscriptionName);
      assertSuccess(result, subscriptionName);
    }));
  });

  describe(`publishManyJson()`, () => {
    const topicName = `lib_test_${uuid.v4()}`;
    const subscriptionName = `lib_test_${uuid.v4()}`;
    const message1 = {
      data: { isOne: true },
      attributes: { today: 'friday' }
    };
    const message2 = {
      data: { isOne: false },
      attributes: { today: 'saturday' }
    };

    it(`create subscriber`, () => {
      _createSubscriber();
    });
    it(`create publisher`, () => {
      _createPublisher();
    });

    it(`should create a topic`, _asyncToGenerator(function* () {
      const result = yield createTopic(topicName);
      assertSuccess(result);
    }));

    it(`should create the subscription`, _asyncToGenerator(function* () {
      const result = yield createSubscription(topicName, subscriptionName);
      assertSuccess(result);
    }));

    it(`should publish many messages`, _asyncToGenerator(function* () {
      const result = yield publishManyJson(topicName, [message1, message2]);
      assertSuccess(result, 2);
    }));

    it(`should pull message`, _asyncToGenerator(function* () {
      // This. Test. Is. Hideous. But I feel it needs to validate both maxMessages
      // and I don't know how I want to flatten the ugly message from GC yet.
      const expected1 = Object.assign({}, message1, {
        data: Buffer.from(JSON.stringify(message1.data))
      });
      const expected2 = Object.assign({}, message2, {
        data: Buffer.from(JSON.stringify(message2.data))
      });
      const maxMessages = 2;
      const returnImmediately = true;
      const result = yield pull(subscriptionName, maxMessages, returnImmediately);
      const [response] = payload(result);
      const { receivedMessages } = response;
      const [msg1, msg2] = receivedMessages;
      const { message: msg1Body } = msg1;
      const { message: msg2Body } = msg2;
      assertSuccess(result);
      equal(expected1, { data: msg1Body.data, attributes: msg1Body.attributes });
      equal(expected2, { data: msg2Body.data, attributes: msg2Body.attributes });
    }));

    it(`delete the topic`, _asyncToGenerator(function* () {
      const result = yield deleteTopic(topicName);
      assertSuccess(result);
    }));

    it(`should delete the subscription`, _asyncToGenerator(function* () {
      const result = yield deleteSubscription(subscriptionName);
      assertSuccess(result, subscriptionName);
    }));
  });

  describe('publishJson()', () => {
    const topicName = `lib_test_${uuid.v4()}`;
    const subscriptionName = `lib_test_${uuid.v4()}`;

    it(`create subscriber`, () => {
      _createSubscriber();
    });
    it(`create publisher`, () => {
      _createPublisher();
    });

    it(`should create a topic`, _asyncToGenerator(function* () {
      const result = yield createTopic(topicName);
      assertSuccess(result);
    }));

    it(`should create the subscription`, _asyncToGenerator(function* () {
      const result = yield createSubscription(topicName, subscriptionName);
      assertSuccess(result);
    }));

    it(`should publish an object message`, _asyncToGenerator(function* () {
      const message = {
        data: { isMessage: true },
        attributes: { metal: 'is sick' }
      };
      const result = yield publishJson(topicName, message);
      assertSuccess(result);
    }));

    it(`should pull message`, _asyncToGenerator(function* () {
      const maxMessages = 1;
      const returnImmediately = true;
      const result = yield pull(subscriptionName, maxMessages, returnImmediately);
      assertSuccess(result);
    }));

    it(`delete the topic`, _asyncToGenerator(function* () {
      const result = yield deleteTopic(topicName);
      assertSuccess(result);
    }));

    it(`should delete the subscription`, _asyncToGenerator(function* () {
      const result = yield deleteSubscription(subscriptionName);
      assertSuccess(result, subscriptionName);
    }));
  });
});