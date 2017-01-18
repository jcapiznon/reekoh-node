'use strict'

let amqp = require('amqplib')
let reekoh = require('../app.js')
let isEqual = require('lodash.isequal')

describe('Storage Plugin Test', () => {
  // --- preparation
  process.env.LOGGERS = ''
  process.env.EXCEPTION_LOGGERS = ''

  process.env.INPUT_PIPE = 'demo.storage'
  process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

  let _plugin = new reekoh.plugins.Storage()
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
    it('should recieve data from input pipe queue', (done) => {
      _channel.sendToQueue(process.env.INPUT_PIPE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('data', (data) => {
        if (!isEqual(dummyData, data)) return should.fail('rcv data must be same')
        done()
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

    it('should send an error if log data is empty', (done) => {

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

