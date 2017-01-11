'use strict'

let async = require('async')
let Messenger = require('../lib/messenger.lib.js')

let EventEmitter = require('events').EventEmitter

class DeviceSync extends EventEmitter {

  constructor (config) {
    super()
    this.config = config
    let brokerConnString = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    async.series({
      broker: (done) => {
        let broker = new Messenger.Broker()

        broker.connect(brokerConnString)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            done(null, broker)
            return null
          })
          .catch((err) => {
            console.error('Could not connect to RabbitMQ Server. ERR:', err)
          })
      }
    }, (err, res) => {
      if (err) {
        return console.error(err)
      }

      let broker = res.broker
      let q1 = new Messenger.Queue(broker, 'q1')
      let q2 = new Messenger.Queue(broker, 'q2')

      let data = `TEST_DATA_${Date.now() + 1}`

      // -- sending data using q1 and q2

      q1.send(`${data}_q1`)
        .then(() => {
          console.log('snd data -> ', `${data}_q1`)
        })
        .catch((err) => {
          console.log(err)
        })
      q2.send(`${data}_q2`)
        .then(() => {
          console.log('snd data -> ', `${data}_q2`)
        })
        .catch((err) => {
          console.log(err)
        })

      // -- consume data using q1 and q2

      q1.consume()
        .then((msg) => {
          console.log('rcv data ->', msg)
        })
        .catch((err) => {
          console.log(err)
        })

      q2.consume()
        .then((msg) => {
          console.log('rcv data ->', msg)
        })
        .catch((err) => {
          console.log(err)
        })
    })
  }

}

module.exports = DeviceSync
