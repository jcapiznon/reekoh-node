/* global describe, it */

'use strict'

const amqp = require('amqplib')
const Reekoh = require('../../index.js')
const isEqual = require('lodash.isequal')

describe('Service Plugin Test', () => {
  let _plugin = null
  let _channel = null
  let _conn = null

  let errLog = (err) => { console.log(err) }

  before('#test init', () => {
    process.env.PLUGIN_ID = 'plugin1'
    process.env.PIPELINE = 'Pl1'
    process.env.OUTPUT_PIPES = 'Op1,Op2'
    process.env.LOGGERS = 'logger1,logger2'
    process.env.EXCEPTION_LOGGERS = 'exlogger1,exlogger2'
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'
    process.env.CONFIG = '{"foo": "bar"}'
    process.env.INPUT_PIPE = 'sip1'
    process.env.OUTPUT_SCHEME = 'MERGE'
    process.env.OUTPUT_NAMESPACE = 'result'
    process.env.ACCOUNT = 'demo account'

    amqp.connect(process.env.BROKER)
      .then((conn) => {
        _conn = conn
        return conn.createChannel()
      }).then((channel) => {
        _channel = channel
      }).catch(errLog)
  })

  after('#terminate connection', (done) => {
    _conn.close()
      .then(() => {
        _plugin.removeAllListeners()
        done()
      })
  })

  describe('#spawn', () => {
    it('should spawn the class without error', function (done) {
      _plugin = new Reekoh.plugins.Service()
      _plugin.once('ready', () => {
        console.log(_plugin.config)
        done()
      })
    })
  })

  describe('#events', () => {
    it('should receive `data` event', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue('sip1', new Buffer(JSON.stringify(dummyData)))

      _plugin.on('data', (data) => {
        if (!isEqual(data, dummyData)) {
          done(new Error('received data not matched'))
        } else {
          done()
        }
      })
    })
  })

  describe('#pipe', () => {
    it('should throw error if data is empty', (done) => {
      _plugin.pipe('', '')
        .then(() => {
          done(new Error('Reject expected.'))
        }).catch((err) => {
          if (!isEqual(err, new Error('Please specify the original data and the result.'))) {
            done(new Error('Return value did not match.'))
          } else {
            done()
          }
        })
    })
  })

  describe('#logging', () => {
    it('should send a log to logger queues', (done) => {
      _plugin.log('dummy log data')
        .then(() => {
          done()
        }).catch(() => {
          done(new Error('send using logger fail.'))
        })
    })

    it('should send an exception log to exception logger queues', (done) => {
      _plugin.logException(new Error('tests'))
        .then(() => {
          done()
        }).catch(() => {
          done(new Error('send using exception logger fail.'))
        })
    })
  })
})
