'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const Broker = require('../broker.lib')

class Service extends EventEmitter {
  constructor () {
    super()

    this.inputPipe = process.env.INPUT_PIPE
    this.outputPipes = process.env.OUTPUT_PIPES.split(',')
    this.loggers = process.env.LOGGERS.split(',')
    this.exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

    let dataEmitter = (data) => {
      async.waterfall([
        async.constant(data.content.toString('utf8')),
        async.asyncify(JSON.parse)
      ], (err, parsed) => {
        if (err) return console.error(err)

        this.emit('data', parsed)
      })
    }

    this.queues = []
    this._broker = new Broker()
    let broker = this._broker

    this.loggers.push('agent.logs')
    this.exceptionLoggers.push('agent.exceptions')

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
        let queueIds = [this.inputPipe]
          .concat(this.outputPipes)
          .concat(this.loggers)
          .concat(this.exceptionLoggers)

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
        // consume input pipe
        this.queues[this.inputPipe].consume((msg) => {
          dataEmitter(msg)
        })
          .then(() => {
            console.log('Input pipes consumed.')
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

      async.each(this.loggers, (logger, done) => {
        this.queues[logger].publish(logData)
          .then(() => {
            console.log(`message written to queue ${logger}`)
            done()
          })
          .catch((error) => {
            done(error)
          })
      }, (error) => {
        if (error) return reject(error)
        resolve()
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
      async.each(this.exceptionLoggers, (logger, done) => {
        this.queues[logger].publish(errData)
          .then(() => {
            console.log(`message written to queue ${logger}`)
            done()
          })
          .catch((error) => {
            done(error)
          })
      }, (error) => {
        if (error) return reject(error)
        resolve()
      })
    })
  }

  pipe (data, result) {
    return new Promise((resolve, reject) => {
      if (isEmpty(data) || isEmpty(result)) return reject(new Error('Please specify the original data and the result.'))

      if (process.env.OUTPUT_SCHEME === 'MERGE') {
        let outputData = JSON.stringify(Object.assign(data, result))

        async.each(this.outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(outputData)
            .then(() => {
              console.log(`message written to queue ${outputPipe}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      } else if (process.env.OUTPUT_SCHEME === 'NAMESPACE') {
        let outputData = JSON.stringify(Object.assign(data[process.env.OUTPUT_NAMESPACE], result))

        async.each(this.outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(outputData)
            .then(() => {
              console.log(`message written to queue ${outputPipe}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      } else if (process.env.OUTPUT_SCHEME === 'RESULT') {
        let outputData = JSON.stringify(result)

        async.each(this.outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(outputData)
            .then(() => {
              console.log(`message written to queue ${outputPipe}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      }
    })
  }
}

module.exports = Service
