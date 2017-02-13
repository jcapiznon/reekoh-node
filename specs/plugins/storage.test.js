/* global describe, it, before, after */

'use strict'

let _conn = null
let _plugin = null
let _channel = null

let amqp = require('amqplib')
let reekoh = require('../../index.js')
let isEqual = require('lodash.isequal')

// preserving.. plugin clears env after init
const ENV_INPUT_PIPE = 'demo.storage'

describe('Storage Plugin Test', () => {
  before('#test init', () => {

    process.env.LOGGERS = ''
    process.env.CONFIG = '{}'
    process.env.EXCEPTION_LOGGERS = ''
    process.env.INPUT_PIPE = ENV_INPUT_PIPE
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

    amqp.connect(process.env.BROKER)
      .then((conn) => {
        _conn = conn
        return conn.createChannel()
      }).then((channel) => {
        _channel = channel
      }).catch((err) => {
        console.log(err)
      })
  })

  after('terminate connection', () => {
    _conn.close()
  })

  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      try {
        _plugin = new reekoh.plugins.Storage()
        done()
      } catch (err) {
        done(err)
      }
    })
  })

  describe('#events', () => {
    it('should rcv `ready` event', (done) => {
      _plugin.once('ready', () => {
        done()
      })
    })

    it('should rcv `data` event', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue(ENV_INPUT_PIPE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('data', (data) => {
        if (!isEqual(dummyData, data)) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })
  })

  describe('#logging', () => {
    describe('.log()', () => {
      it('should throw error if logData is empty', (done) => {
        _plugin.log('')
          .then(() => {
            done(new Error('Expecting rejection. Check your test data.'))
          }).catch((err) => {
            if (!isEqual(err.message, 'Kindly specify the data to log')) {
              done(new Error('Returned value not matched.'))
            } else {
              done()
            }
          })
      })

      it('should send a log to logger queues', (done) => {
        _plugin.log('dummy log data')
          .then(() => {
            done()
          }).catch((err) => {
            done(err)
          })
      })
    })

    describe('.logException()', () => {
      it('should throw error if param is not an Error instance', (done) => {
        _plugin.logException('')
          .then(() => {
            done(new Error('Expecting rejection. Check your test data.'))
          }).catch((err) => {
            if (!isEqual(err.message, 'Kindly specify a valid error to log')) {
              done(new Error('Returned value not matched.'))
            } else {
              done()
            }
          })
      })
      it('should send an exception log to exception logger queues', (done) => {
        _plugin.logException(new Error('test'))
          .then(() => {
            done()
          }).catch((err) => {
            done(err)
          })
      })
    })
  })
})

