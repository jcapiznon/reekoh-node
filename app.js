'use strict'

exports.plugins = {
  Stream: require('./plugins/stream'),
  Channel: require('./plugins/channel'),
  Storage: require('./plugins/storage'),
  DeviceSync: require('./plugins/device-sync'),
  Connector: require('./plugins/connector'),
  ExceptionLogger: require('./plugins/exception-logger'),
  Gateway: require('./plugins/gateway'),
  Logger: require('./plugins/logger'),
  Service: require('./plugins/service')
}
