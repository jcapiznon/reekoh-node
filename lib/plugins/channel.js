'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isEmpty = require('lodash.isempty')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')

const Broker = require('../broker.lib')

class Channel extends EventEmitter {

  constructor () {
    super()
    this.config = {}
    this.port = process.env.PORT || 8080

    const ENV_CA = process.env.CA || ''
    const ENV_CRL = process.env.CRL || ''
    const ENV_KEY = process.env.KEY || ''
    const ENV_CERT = process.env.CERT || ''
    const ENV_ACCOUNT = process.env.ACCOUNT || ''
    const ENV_PIPELINE = process.env.PIPELINE || ''

    const QN_LOGS = 'logs'
    const QN_MESSAGES = 'messages'
    const QN_EXCEPTIONS = 'exceptions'
    const QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.channel'
    const QN_INPUT_PIPE = process.env.INPUT_PIPE || 'demo.channel.pipe'

    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // grouping queue names for easy access
    let _qGroups = {
      loggers: [QN_LOGS],
      exceptionLoggers: [QN_EXCEPTIONS],
      common: [QN_INPUT_PIPE, QN_MESSAGES]
    }

    // env clean up, avoiding disclosure
    process.env.CA = undefined
    process.env.CRL = undefined
    process.env.KEY = undefined
    process.env.CERT = undefined
    process.env.PORT = undefined
    process.env.BROKER = undefined
    process.env.CONFIG = undefined
    process.env.ACCOUNT = undefined
    process.env.LOGGERS = undefined
    process.env.PIPELINE = undefined
    process.env.PLUGIN_ID = undefined
    process.env.INPUT_PIPE = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    // adding custom queues in '_qGroups.*'
    _qGroups.loggers = _qGroups.loggers.concat(_loggerIDs.split(','))
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.concat(_exLoggerIDs.split(','))

    // removing empty elements if any
    _qGroups.loggers = _qGroups.loggers.filter(Boolean)
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.filter(Boolean)

    async.series([
      // connecting to rabbitMQ
      (done) => {
        _broker.connect(_brokerConnStr).then(() => {
          return done() || null // promise warning fix
        }).catch(done)
      },

      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(_config.toString('utf8')),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          this.config = parsed
          done(err)
        })
      },

      // prepare certs/keys needed by SDK plugin developer
      (done) => {
        let root = process.cwd()
        let home = path.join(root, 'keys')

        if (!fs.existsSync(home)) fs.mkdirSync(home)

        let writer = (content, filePath, cb) => {
          if (isEmpty(content)) return cb(null, '')

          fs.writeFile(filePath, content, (err) => { cb(err, filePath) })
        }

        async.series({
          ca: (cb) => { writer(ENV_CA, path.join(home, 'server-ca.pem'), cb) },
          crl: (cb) => { writer(ENV_CRL, path.join(home, 'server-crl.pem'), cb) },
          key: (cb) => { writer(ENV_KEY, path.join(home, 'server-key.pem'), cb) },
          cert: (cb) => { writer(ENV_CERT, path.join(home, 'server-cert.pem'), cb) }
        }, (err, res) => {
          Object.assign(this, res)
          done(err)
        })
      },

      // setting up needed queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_qGroups.common)
        queueIDs = queueIDs.concat(_qGroups.loggers)
        queueIDs = queueIDs.concat(_qGroups.exceptionLoggers)

        async.each(queueIDs, (loggerId, cb) => {
          if (isEmpty(loggerId)) return cb()

          _broker.createQueue(loggerId).then(() => {
            return cb() || null // promise warning fix
          }).catch((err) => {
            console.error('Channel newQueue() ', err)
            cb(err)
          })
        }, done)
      },

      // set topic exchnage queue and consume
      (done) => {
        let processTopicData = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (err) return console.error('Channel processQueue() rcvd data is not a JSON.', err)

              this.emit('data', parsed)
            })
          }
        }

        _broker.createExchange(QN_PLUGIN_ID).then((exchange) => {
          return exchange.consume(processTopicData)
        }).then(() => {
          return done() || null // promise warning fix
        }).catch(done)
      },

      // listen to input pipe, then relay to plugin queue
      (done) => {
        _broker.queues[QN_INPUT_PIPE].consume((msg) => {
          _broker.exchanges[QN_PLUGIN_ID].publish(msg.content.toString('utf8'))
        }).then(() => {
          return done()
        }).catch(done)
      }

    ], (err) => {
      if (err) {
        console.error('Channel: ', err)
        throw err
      }

      process.nextTick(() => {
        // plugin initialized
        this.emit('ready')
      })
    })

    this.relayMessage = (message, deviceTypes, devices) => {
      return new BPromise((resolve, reject) => {
        if (!message) {
          return reject(new Error('Kindly specify the command/message to send'))
        }

        if (!isString(message)) {
          return reject(new Error('Message must be a string'))
        }

        if (isEmpty(devices) && isEmpty(deviceTypes)) {
          return reject(new Error('Kindly specify the target device types or devices'))
        }

        if (!(isString(deviceTypes) || Array.isArray(deviceTypes)) || !(isString(devices) || Array.isArray(devices))) {
          return reject(new Error(`'deviceTypes' and 'devices' must be a string or an array.`))
        }

        _broker.queues[QN_MESSAGES].publish({
          pipeline: ENV_PIPELINE,
          message: message,
          deviceTypes: deviceTypes,
          devices: devices
        }).then(() => {
          resolve()
        }).catch(reject)

      })
    }

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

        let msg = null

        // loggers and custom loggers are in _qGroups.loggers array
        async.each(_qGroups.loggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = (loggerId === QN_LOGS) ? {account: ENV_ACCOUNT, data: logData} : logData

          // publish() has a built in stringify, so objects are safe to feed
          _broker.queues[loggerId].publish(msg).then(() => {
            callback()
          }).catch(callback)

        }, (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }

    this.logException = (err) => {
      return new BPromise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Kindly specify a valid error to log'))

        let msg = null

        let errData = {
          name: err.name,
          message: err.message,
          stack: err.stack
        }

        async.each(_qGroups.exceptionLoggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = (loggerId === QN_EXCEPTIONS) ? {account: ENV_ACCOUNT, data: errData} : errData

          _broker.queues[loggerId].publish(msg).then(() => {
            callback()
          }).catch(callback)

        }, (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }
  }
}

module.exports = Channel
