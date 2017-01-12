'use strict'

let reekoh = require('./app.js')
let deviceSync = new reekoh.plugins.DeviceSync()

let isEmpty = require('lodash.isempty')

deviceSync.once('ready', function () {
  console.log('Plugin Ready!')
})

deviceSync.on('sync', function () {
  /*********
    TODO: get devices from 3rd party, then feed to the platform using syncDevice()
    Sample Code:
    client.fetchData(function (error, dataCollection) {
      dataCollection.forEach(function (data) {
        platform.syncDevice(data);
      });
    });
   *********/
  // TEST DATA: {"operation":"sync", "_id":123, "name":"device-123"}
  deviceSync.syncDevice({
    _id: 123,
    name: 'device123'
  }).then(() => {
    return console.log('PLUGIN: sync ok!') || null
  }).catch((err) => {
    console.error('PLUGIN:', err)
  })
})

deviceSync.on('adddevice', function (device) {
  console.log('adddevice:', device)
})

deviceSync.on('updatedevice', function (device) {
  console.log('updatedevice:', device)
})

deviceSync.on('removedevice', function (device) {
  // TEST DATA: {"operation":"removedevice", "device":{"_id":123, "name":"device-123"}}

  if (isEmpty(device)) return console.error('PLUGIN: empty device', device)

  deviceSync.removeDevice(device._id).then(() => {
    return console.log('PLUGIN: remove ok!')
  }).catch((err) => {
    console.error('PLUGIN:', err)
  })

  deviceSync.log('dummy log data')
    .then(() => {
      return console.log('PLUGIN: log ok!')
    }).catch((err) => {
      console.error('PLUGIN:', err)
    })

  deviceSync.logException(new Error('test'))
    .then(() => {
      return console.log('PLUGIN: logException ok!')
    }).catch((err) => {
      console.error('PLUGIN:', err)
    })
})
