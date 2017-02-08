'use strict'

let amqp = require('amqplib')
let Reekoh = require('../../index.js')
let isEqual = require('lodash.isequal')

let _conn = null
let _plugin = null
let _channel = null

const ERR_RETURN_UNMATCH = 'Returned value not matched.'
const ERR_EMPTY_LOG_DATA = 'Please specify a data to log.'

describe('Exception Logger Plugin Test', () => {
  before('#test init', () => {
    process.env.INPUT_PIPE = 'lipexcp.1'
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'
    process.env.CONFIG = '{"foo":"bar"}'

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
      .then(() => {
        _plugin.removeAllListeners()
      })
  })

  // --- tests
  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      _plugin = new Reekoh.plugins.ExceptionLogger()
      _plugin.once('ready', () => {
        done()
      })
    })
  })

  describe('#events', () => {
    it('should receive data from input pipe queue', (done) => {
      let dummyData = new Error('test error')
      _channel.sendToQueue('lipexcp.1', new Buffer(JSON.stringify(dummyData)))

      _plugin.on('exception', (data) => {
        done()
      })
    })
  })

  describe('#logging', () => {
    it('should throw error if logData is empty', (done) => {
      _plugin.log('')
        .then(() => {
          done()
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_LOG_DATA)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should throw error if param is not an Error instance', (done) => {
      _plugin.logException('')
        .then(() => {
          done()
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_LOG_DATA)) {
            done(new Error(ERR_RETURN_UNMATCH))
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

