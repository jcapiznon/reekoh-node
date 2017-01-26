'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const Promise = require('bluebird')

const isArray = require('lodash.isarray')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')

const Broker = require('../lib/broker.lib.js')
const EventEmitter = require('events').EventEmitter

class Channel extends EventEmitter {

  constructor () {
    super()

    this.config = {}
    this.queues = []

    this.QN_AGENT_MESSAGES = 'agent.messages'
    this.QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.channel'
    this.QN_INPUT_PIPE = process.env.INPUT_PIPE || 'demo.channel.pipe'

    this.qn = {
      common: [
        this.QN_INPUT_PIPE,
        this.QN_AGENT_MESSAGES
      ],
      loggers: ['agent.logs'],
      exceptionLoggers: ['agent.exceptions']
    }

    let _self = this
    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // preparing logger queue names in array
    _self.qn.exceptionLoggers = _self.qn.exceptionLoggers.concat(_exLoggerIDs.split(','))
    _self.qn.loggers = _self.qn.loggers.concat(_loggerIDs.split(','))

    // removing empty elements if any
    _self.qn.exceptionLoggers = _self.qn.exceptionLoggers.filter(Boolean)
    _self.qn.loggers = _self.qn.loggers.filter(Boolean)

    _self.port = process.env.PORT || 8080

    async.waterfall([
      // connecting to rabbitMQ
      (done) => {
        _broker.connect(_brokerConnStr)
          .then(() => {
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(_config.toString('utf8')),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          _self.config = parsed
          done(err)
        })
      },

      // prepare certs/keys needed by SDK plugin developer
      (done) => {
        let root = process.cwd()
        let home = path.join(root, 'keys')

        if (!fs.existsSync(home)) fs.mkdirSync(home)

        let writer = (content, filePath, callback) => {
          if (isEmpty(content)) return callback(null, '')
          fs.writeFile(filePath, content, (err) => { callback(err, filePath) })
        }

        async.series({
          ca: (callback) => { writer(process.env.CA, path.join(home, 'server-ca.pem'), callback) },
          crl: (callback) => { writer(process.env.CRL, path.join(home, 'server-crl.pem'), callback) },
          key: (callback) => { writer(process.env.KEY, path.join(home, 'server-key.pem'), callback) },
          cert: (callback) => { writer(process.env.CERT, path.join(home, 'server-cert.pem'), callback) }
        }, (err, res) => {
          Object.assign(_self, res)
          done(err)
        })
      },

      // setting up generic queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_self.qn.common)
        queueIDs = queueIDs.concat(_self.qn.loggers)
        queueIDs = queueIDs.concat(_self.qn.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              if (queue) _self.queues[loggerId] = queue
              return callback() || null // !
            }).catch((err) => {
              console.error('Channel newQueue()', err)
            })
        }, (err) => {
          done(err)
        })
      },

      // set topic exchnage queue and consume
      (done) => {
        let queueName = _self.QN_PLUGIN_ID

        let processTopicData = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (!err) return _self.emit('data', parsed)
              console.error('Channel processQueue() rcvd data is not a JSON.', err)
            })
          }
        }

        _broker.newExchangeQueue(queueName)
          .then((queue) => {
            if (!queue) throw new Error('newExchangeQueue() fail') // will be catch by async
            _self.queues[queueName] = queue
            return queue
          }).then((queue) => {
            return queue.consume(processTopicData)
          }).then(() => {
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // listen to input pipe, then relay to plugin queue
      (done) => {
        let pipeQueue = _self.queues[_self.QN_INPUT_PIPE]
        let pluginQueue = _self.queues[_self.QN_PLUGIN_ID]

        pipeQueue.consume((msg) => { pluginQueue.publish(msg.content.toString('utf8')) })
          .then((msg) => {
            return done() || null
          }).catch((err) => {
            done(err)
          })
      }

    ], (err) => {
      if (err) return console.error('Channel: ', err)

      // plugin initialized
      _self.emit('ready')
    })
  }

  relayMessage (message, deviceTypes, devices) {
    let queueName = this.QN_AGENT_MESSAGES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!message) {
        return reject(new Error('Kindly specify the command/message to send'))
      }
      if (isEmpty(devices) && isEmpty(deviceTypes)) {
        return reject(new Error('Kindly specify the target device types or devices'))
      }

      if ((isString(devices) || isArray(devices)) &&
        (isString(deviceTypes) || isArray(deviceTypes))) {
        let data = JSON.stringify({
          pipeline: process.env.PIPELINE,
          message: message,
          deviceTypes: deviceTypes,
          devices: devices
        })

        queue.publish(data)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      } else {
        return reject(new Error("'devices' and 'deviceTypes' must be a string or an array."))
      }
    })
  }

  log (logData) {
    let self = this

    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) {
        return reject(new Error('Kindly specify the data to log'))
      }

      // loggers and custom loggers are in self.loggers array
      async.each(self.qn.loggers, (loggerId, callback) => {
        if (!loggerId) return callback()

        // publish() has a built in stringify, so objects are safe to feed
        self.queues[loggerId].publish(logData)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  logException (err) {
    let self = this

    return new Promise((resolve, reject) => {
      if (!(err instanceof Error)) {
        return reject(new Error('Kindly specify a valid error to log'))
      }

      let data = JSON.stringify({
        name: err.name,
        message: err.message,
        stack: err.stack
      })

      async.each(self.qn.exceptionLoggers, (loggerId, callback) => {
        if (!loggerId) return callback()

        self.queues[loggerId].publish(data)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

}

module.exports = Channel
