'use strict'

let amqp = require('amqplib')
let reekoh = require('../app.js')
let isEqual = require('lodash.isequal')

describe('Channel Plugin Test', () => {
  // --- preparation
  process.env.LOGGERS = ''
  process.env.EXCEPTION_LOGGERS = ''

  process.env.INPUT_PIPE = 'demo.pipe.channel'
  process.env.PLUGIN_ID = 'demo.plugin.channel'
  process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

  let _plugin = new reekoh.plugins.Channel()
  let _channel = null

  let dummyData = { foo: 'bar' }
  let errLog = (err) => { console.log(err) }

  amqp.connect(process.env.BROKER)
    .then((conn) => {
      return conn.createChannel()
    }).then((channel) => {
      _channel = channel
    }).catch(errLog)

  // --- tests

  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      _plugin.once('ready', () => {
        done()
      })
    })
  })

  describe('#events', () => {
    it('should rcv `data` event', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue(process.env.INPUT_PIPE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('data', (data) => {
        if (!isEqual(data, dummyData)) {
          done(new Error('Rcvd data not matched'))
        } else {
          done()
        }
      })
    })
  })

  describe('#relayMessage()', () => {
    it('should throw error if message is empty', (done) => {
      _plugin.relayMessage('', '', '')
        .then(() => {
          done(new Error('relayMessage() with empy message param should fail'))
        }).catch((err) => {
          if (!isEqual(err, new Error('Kindly specify the command/message to send'))) {
            done(new Error('relayMessage() returned error not matched'))
          } else {
            done()
          }
        })
    })

    it('should throw error if device or deviceTypes is empty', (done) => {
      _plugin.relayMessage('test', '', '')
        .then(() => {
          done(new Error('relayMessage() with empy device or deviceTypes param should fail'))
        }).catch((err) => {
          if (!isEqual(err, new Error('Kindly specify the target device types or devices'))) {
            done(new Error('relayMessage() returned error not matched'))
          } else {
            done()
          }
        })
    })

    it('should publish a message to pipeline', (done) => {
      _plugin.relayMessage('test', ['a'], ['b'])
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
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
