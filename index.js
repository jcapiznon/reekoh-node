'use strict'

let reekoh = require('./app.js')

let deviceSync = new reekoh.plugins.DeviceSync()

deviceSync.once('ready', function (msg) {
  console.log('ready:', msg)
})

deviceSync.on('message', function (msg) {
  console.log('message:', msg)
})
