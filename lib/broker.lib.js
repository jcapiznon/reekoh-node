'use strict'

const amqp = require('amqplib')
const uuid = require('uuid/v4')
const BPromise = require('bluebird')
const isEmpty = require('lodash.isempty')
const isPlainObject = require('lodash.isplainobject')

class Queue {
  constructor (channel, queueName) {
    this.channel = channel
    this.queueName = queueName
  }

  publish (data, options) {
    let channel = this.channel
    let queueName = this.queueName

    if (isPlainObject(data)) data = JSON.stringify(data)

    if (isPlainObject(options)) {
      Object.assign(options, {
        persistent: false
      })
    } else if (isEmpty(options)) {
      options = {
        persistent: false
      }
    }

    return new BPromise((resolve, reject) => {
      if (!channel.sendToQueue(queueName, new Buffer(data, 'utf8'), options)) {
        reject(new Error('Queue.publish() failed!'))
      } else {
        resolve()
      }
    })
  }

  consume (callback) {
    let channel = this.channel
    let queueName = this.queueName

    return channel.consume(queueName, (msg) => {
      if (!msg) return
      callback(msg)
      channel.ack(msg)
    })
  }
}

class RpcQueue {
  constructor (channel, queueName, generatedQueueName) {
    this.channel = channel
    this.queueName = queueName
    this.generatedQueueName = generatedQueueName
  }

  // used for testing only. rpc server should be in platform
  serverConsume (promise) {
    let channel = this.channel
    let queueName = this.queueName

    return channel.consume(queueName, (msg) => {
      if (!msg) return

      promise(msg).then((response) => {
        let data = new Buffer(response.toString())

        let options = {
          correlationId: msg.properties.correlationId,
        }

        channel.sendToQueue(msg.properties.replyTo, data, options)
        channel.ack(msg)
      }).catch((err) => {
        console.error(err)
      })
    })
  }

  publish (data) {
    let corrId = uuid()
    let channel = this.channel
    let queueName = this.queueName
    let generatedQueueName = this.generatedQueueName

    return new BPromise((resolve, reject) => {
      let answer = (msg) => {
        if (msg.properties.correlationId === corrId) {
          resolve(msg.content.toString())
        }
      }

      return channel.consume(generatedQueueName, answer, {noAck: true}).then(() => {
        if (isPlainObject(data)) data = JSON.stringify(data)

        channel.sendToQueue(queueName, new Buffer(data, 'utf8'), {
          correlationId: corrId,
          replyTo: generatedQueueName,
          persistent: false
        })
      }).catch((err) => {
        reject(err)
      })
    })
  }
}

class Broker {
  constructor () {
    this.channel = null
    this.connection = null
    this.queues = {}
    this.exchanges = {}
    this.rpcs = {}
  }

  connect (connString) {
    return new BPromise((resolve, reject) => {
      amqp.connect(connString).then((conn) => {
        this.connection = conn
        return conn.createChannel()
      }).then((channel) => {
        this.channel = channel
        resolve()
      }).catch(reject)
    })
  }

  createQueue (queueName) {
    return new BPromise((resolve, reject) => {
      return this.channel.assertQueue(queueName).then(() => {
        let queue = new Queue(this.channel, queueName)

        this.queues[queueName] = queue

        resolve(queue)
      }).catch(reject)
    })
  }

  createExchange (exchangeName, routingKey) {
    return new BPromise((resolve, reject) => {
      return this.channel.assertExchange('amq.topic', 'topic', routingKey || exchangeName).then(() => {
        return this.channel.assertQueue(exchangeName)
      }).then((ret) => {
        return this.channel.bindQueue(ret.queue, exchangeName, routingKey || exchangeName)
      }).then(() => {
        let exchange = new Queue(this.channel, exchangeName)

        this.exchanges[exchangeName] = exchange

        resolve(exchange)
      }).catch(reject)
    })
  }

  createRPC (type, queueName) {
    let channel = this.channel

    let option = ''
    let toAssert = queueName
    let isServer = (type === 'server')
    let isClient = (type === 'client')

    return new BPromise((resolve, reject) => {
      if (isServer) {
        option = {durable: false}
      }
      if (isClient) {
        toAssert = ''
        option = {exclusive: true}
      }

      return channel.assertQueue(toAssert, option).then((ret) => {
        channel.prefetch(1)
        return isClient ? ret.queue : queueName
      }).then((retQueue) => {
        let rpc = new RpcQueue(this.channel, queueName, retQueue)

        this.rpcs[queueName] = rpc

        resolve(rpc)
      }).catch(reject)
    })
  }
}

module.exports = Broker
