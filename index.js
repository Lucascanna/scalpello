'use strict'

const { start } = require('./lib/core')

const exampleConfig = {
  source: {
    protocol: 'http',
    fileType: 'csv',
    url: 'http://www.sir.toscana.it/archivio/download.php?IDST=pluvio&IDS=TOS01000581'
  },
  target: {
    type: 'mongodb',
    config: {
      mongodbUrl: 'mongodb://localhost:27017',
      dbName: 'test',
      collectionName: 'stations'
    }
  }
}

start(exampleConfig)

