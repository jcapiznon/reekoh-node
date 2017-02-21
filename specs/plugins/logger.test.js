'use strict'

let amqp = require('amqplib')
let Reekoh = require('../../index.js')
let isEqual = require('lodash.isequal')


describe('Logger Plugin Test', () => {
  let _conn = null
  let _plugin = null
  let _channel = null

  let errLog = (err) => { console.log(err) }

  before('#test init', () => {
    process.env.INPUT_PIPE = 'lip.1'
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
      _plugin = new Reekoh.plugins.Logger()
      _plugin.once('ready', () => {
        console.log(_plugin.config)
        done()
      })
    })
  })

  describe('#events', () => {
    it('should receive data from input pipe queue', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue('lip.1', new Buffer(JSON.stringify(dummyData)))

      _plugin.on('log', (data) => {
        if (!isEqual(data, dummyData)) {
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

