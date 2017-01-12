'use strict'

let amqp = require('amqplib')
let Promise = require('bluebird')
let isPlainObject = require('lodash.isplainobject')

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
        reject(new Error('Queue.send() failed!'))
      } else {
        resolve()
      }
    })
  }

  consume (callback) {
    let channel = this.channel
    let queueName = this.queueName

    return channel.consume(queueName, (msg) => {
      if (msg !== null) {
        callback(msg)
        channel.ack(msg)
      }
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
}

module.exports = Broker
