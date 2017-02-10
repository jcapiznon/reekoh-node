'use strict'

const amqp = require('amqplib')
const uuid = require('uuid/v4')
const Promise = require('bluebird')
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

    return new Promise((resolve, reject) => {
      if (!channel.sendToQueue(queueName, new Buffer(data), options)) {
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

  // used for testing only. rpc server should be in agent platformt
  serverConsume (promise) {
    let channel = this.channel
    let queueName = this.queueName

    return channel.consume(queueName, (msg) => {
      if (!msg) return

      promise(msg)
        .then((response) => {
          let data = new Buffer(response.toString())
          let options = {correlationId: msg.properties.correlationId}
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

    return new Promise((resolve, reject) => {
      let answer = (msg) => {
        if (msg.properties.correlationId === corrId) {
          resolve(msg.content.toString())
        }
      }

      return channel.consume(generatedQueueName, answer, {noAck: true})
        .then(() => {
          if (isPlainObject(data)) data = JSON.stringify(data)
          channel.sendToQueue(queueName, new Buffer(data), {
            correlationId: corrId, replyTo: generatedQueueName
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
  }

  connect (connString) {
    return new Promise((resolve, reject) => {
      amqp.connect(connString)
        .then((conn) => {
          this.connection = conn
          return conn.createChannel()
        }).then((channel) => {
          this.channel = channel
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  newQueue (queueName) {
    return new Promise((resolve, reject) => {
      return this.channel.assertQueue(queueName)
        .then(() => {
          resolve(new Queue(this.channel, queueName))
        }).catch((err) => {
          reject(err)
        })
    })
  }

  newExchangeQueue (exchangeName, exchangeType) {
    return new Promise((resolve, reject) => {
      return this.channel.assertExchange(exchangeName, exchangeType)
        .then((exchange) => {
          return this.channel.assertQueue(exchangeName)
        }).then((ret) => {
          return this.channel.bindQueue(ret.queue, exchangeName)
        }).then(() => {
          resolve(new Queue(this.channel, exchangeName))
        })
        .catch((error) => {
          reject(error)
        })
    })
  }

  newRpc (type, queueName) {
    let channel = this.channel

    let option = ''
    let toAssert = queueName
    let isServer = (type === 'server')
    let isClient = (type === 'client')

    return new Promise((resolve, reject) => {
      if (isServer) {
        option = { durable: false }
      }
      if (isClient) {
        toAssert = ''
        option = { exclusive: true }
      }

      return channel.assertQueue(toAssert, option)
        .then((ret) => {
          channel.prefetch(1)
          return isClient ? ret.queue : queueName
        }).then((retQueue) => {
          resolve(new RpcQueue(this.channel, queueName, retQueue))
        }).catch((err) => {
          reject(err)
        })
    })
  }
}

module.exports = Broker
