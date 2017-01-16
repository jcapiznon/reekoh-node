'use strict'

let Promise = require('bluebird')
let EventEmitter = require('events').EventEmitter
let async = require('async')
let isEmpty = require('lodash.isempty')
let Broker = require('../lib/broker.lib')
let inputPipes = process.env.INPUT_PIPES.split(',')

class ExceptionLogger extends EventEmitter {
  constructor () {
    super()

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
        // connect to rabbitmq  1.
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
        let queueIds = inputPipes
          .concat('agent.logs')
          .concat('agent.exceptions')

        console.log(queueIds)

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
        // consume input pipes
        async.each(inputPipes, (inputPipe, callback) => {
          this.queues[inputPipe].consume((msg) => {
            dataEmitter(msg)
          })
            .then(() => {
              callback()
            })
            .catch((error) => {
              console.log(error)
              callback(error)
            })
        }, (error) => {
          if (!error) console.log('Input pipes consumed.')
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
      this.queues['agent.exceptions'].publish(errData)
        .then(() => {
          console.log('message written to agent.exceptions')
        })
        .catch((error) => {
          console.error(error)
        })
    })
  }
}

module.exports = ExceptionLogger
