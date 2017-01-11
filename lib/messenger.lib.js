'use strict'

let amqp = require('amqplib')
let Promise = require('bluebird')
let isEmpty = require('lodash.isempty')

class Broker {
  constructor () {
    this._channel = null
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
          this._channel = channel
          resolve()
        })
        .catch((err) => {
          reject(err)
        })
    })
  }

  assert (queueName) {
    return new Promise((resolve, reject) => {
      this._channel.assertQueue(queueName)
        .then(() => {
          resolve()
        })
        .catch((error) => {
          reject(error)
        })
    })
  }

  send (queueName, message) {
    return new Promise((resolve, reject) => {
      let success = this._channel.sendToQueue(queueName, message)

      if(!success) return reject(new Error('Error sending to queue.'))

      resolve()
    })
  }

  consume(queueName) {
    return new Promise((resolve, reject) => {
      this._channel.consume(queueName, (msg) => {
        if(isEmpty(msg)) resolve(null)
        else{
          resolve(msg.content.toString())
          channel.ack(msg)
        }
      })
    })
  }
}

module.exports = Broker
