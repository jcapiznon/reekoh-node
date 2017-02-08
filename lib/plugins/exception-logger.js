'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const Broker = require('../broker.lib')


class ExceptionLogger extends EventEmitter {
  constructor () {
    super()
    this.inputPipe = process.env.INPUT_PIPE

    let dataEmitter = (msg) => {
      async.waterfall([
        async.constant(msg.content.toString('utf8')),
        async.asyncify(JSON.parse)
      ], (err, parsed) => {
        if (err) return console.error(err)
        this.emit('exception', parsed)
      })
    }

    this.queues = []
    this._broker = new Broker()
    let broker = this._broker

    async.waterfall([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          done(err)
          this.config = parsed
        })
      },
      (done) => {
        // connect to rabbitmq
        broker.connect(process.env.BROKER)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        let queueIds = [ this.inputPipe, 'agent.logs', 'agent.exceptions' ]
        async.each(queueIds, (queueID, callback) => {
          broker.newQueue(queueID)
            .then((queue) => {
              this.queues[queueID] = queue
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          if (!error) console.log('Connected to queues.')
          done(error)
        })
      },
      (done) => {
        this.queues[this.inputPipe].consume((msg) => {
          console.log(this.inputPipe)
          dataEmitter(msg)
        })
          .then(() => {
            done()
          })
          .catch((error) => {
            done(error)
          })
      }
    ], (error) => {
      if (error) return console.error(error)
      console.log('Plugin init process done.')
      this.emit('ready')
    })
  }

  log (logData) {
    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))
      this.queues['agent.logs'].publish(logData)
        .then(() => {
          console.log('message written to agent.logs')
        })
        .catch((error) => {
          console.error(error)
        })
    })
  }

  logException (err) {
    let errData = {
      name: err.name,
      message: err.message,
      stack: err.stack
    }

    return new Promise((resolve, reject) => {
      if (!(err instanceof Error)) return reject(new Error(`Please specify a data to log.`))
      this.queues['agent.exceptions'].publish(errData)
        .then(() => {
          resolve()
        })
        .catch((error) => {
          reject(error)
        })
    })
  }
}

module.exports = ExceptionLogger
