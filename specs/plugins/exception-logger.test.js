'use strict'

let amqp = require('amqplib')
let Reekoh = require('../../index.js')
let isEqual = require('lodash.isequal')

describe('Exception Logger Plugin Test', () => {
  let _conn = null
  let _plugin = null
  let _channel = null

  let errLog = (err) => { console.log(err) }

  before('#test init', () => {
    process.env.INPUT_PIPE = 'lipexcp.1'
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'
    process.env.CONFIG = '{"foo":"bar"}'
    process.env.ACCOUNT = 'demo account'

    amqp.connect(process.env.BROKER)
      .then((conn) => {
        _conn = conn
        return conn.createChannel()
      }).then((channel) => {
      _channel = channel
    }).catch(errLog)
  })

  after('terminate connection', () => {
    _conn.close()
      .then(() => {
        _plugin.removeAllListeners()
      })
  })

  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      _plugin = new Reekoh.plugins.ExceptionLogger()
      _plugin.once('ready', () => {
        console.log(_plugin.config)
        done()
      })
    })
  })

  describe('#events', () => {
    it('should receive data from input pipe queue', (done) => {
      let dummyData = new Error('test')
      _channel.sendToQueue('lipexcp.1', new Buffer(JSON.stringify({message: dummyData.message, stack: dummyData.stack, name:dummyData.name})))

      _plugin.on('exception', (data) => {
        if (!isEqual(data, {message: dummyData.message, stack: dummyData.stack, name:dummyData.name})) {
          done(new Error('received data not matched'))
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
      _plugin.logException(new Error('test'))
        .then(() => {
          done()
        }).catch(() => {
        done(new Error('send using exception logger fail.'))
      })
    })
  })
})
