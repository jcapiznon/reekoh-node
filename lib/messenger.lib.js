'use strict'

let amqp = require('amqplib')
let Promise = require('bluebird')

class Broker {
  constructor () {
    this.channel = null
    this.connnection = null
  }
  connect (connString) {
    return new Promise((resolve, reject) => {
      amqp.connect(connString)
        .then((conn) => {
          this.connection = conn
          return conn.createChannel()
        })
        .then((channel) => {
          this.channel = channel
          resolve()
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
}

class Queue {
  constructor (broker, queueName) {
    this.channel = broker.channel
    this.queueName = queueName
  }
  send (data, options) {
    let channel = this.channel
    let queueName = this.queueName

    return new Promise((resolve, reject) => {
      channel.assertQueue(queueName)
        .then(() => {
          let success = channel.sendToQueue(queueName, new Buffer(JSON.stringify(data)), options)
          if (!success) {
            reject(new Error('Broker.sendToQueue() failed!'))
          } else {
            resolve()
          }
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
  consume (options) {
    let channel = this.channel
    let queueName = this.queueName

    return new Promise((resolve, reject) => {
      channel.assertQueue(queueName)
        .then(() => {
          channel.consume(queueName, (msg) => {
            if (msg === null) {
              resolve(null)
            } else {
              resolve(msg.content.toString())
              channel.ack(msg)
            }
          }, options)
          return null
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
}

module.exports.Broker = Broker
module.exports.Queue = Queue
