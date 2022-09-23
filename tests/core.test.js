'use strict'

const tap = require('tap')
const nock = require('nock')
const { MongoClient } = require('mongodb')

const { start } = require('../lib/core.js')

const setupMongoCollection = async (mongodbUrl, dbName, collectionName, initialContent) => {
  const client = new MongoClient(mongodbUrl)
  await client.connect()
  const db = client.db(dbName)
  const collection = db.collection(collectionName)
  await collection.deleteMany()
  await collection.insertMany(initialContent)
  return client
}

tap.test('simple scraping from csv to mongo', async(test) => {
  const config = {
    source: {
      url: 'http://www.sir.toscana.it/archivio/download.php?IDST=pluvio&IDS=TOS01000581'
    },
    target: {
      config: {
        mongodbUrl: 'mongodb://localhost:27017',
        dbName: 'test',
        collectionName: 'stations'
      }
    }
  }

  const { url } = config.source
  const parsedUrl = new URL(url)
  const { mongodbUrl, dbName, collectionName } = config.target.config
  const sourceMock = nock(parsedUrl.origin)
    .get(`${parsedUrl.pathname}${parsedUrl.search}`)
    .replyWithFile(200, './tests/data/prec_simple.csv', { 'Content-Type': 'text/csv' })

  const initialMongoContent = [
    {
      'gg/mm/aaaa': '10/06/1994',
      'Precipitazione [mm]': '0,4',
      'Tipo Dato': 'V'
    },
  ]
  
  const mongoClient = await setupMongoCollection(mongodbUrl, dbName, collectionName, initialMongoContent)
  await start(config)

  const expectedMongoContent = [
    {
      'gg/mm/aaaa': '10/06/1994',
      'Precipitazione [mm]': '0,4',
      'Tipo Dato': 'V'
    },
    {
      'gg/mm/aaaa': '11/06/1994',
      'Precipitazione [mm]': '2,4',
      'Tipo Dato': 'V'
    },
    {
      'gg/mm/aaaa': '12/06/1994',
      'Precipitazione [mm]': '21,0',
      'Tipo Dato': 'V'
    }
  ]
  const actualMongoContent = (await mongoClient.db(dbName).collection(collectionName).find().toArray())
  // eslint-disable-next-line no-unused-vars
    .map(({ _id, ...fields }) => fields)
  
  test.strictSame(actualMongoContent, expectedMongoContent)
  test.ok(sourceMock.isDone())

  await mongoClient.close()
})

tap.test('simple scraping with field renaming from csv to mongo', async(test) => {
  const config = {
    source: {
      url: 'http://www.sir.toscana.it/archivio/download.php?IDST=pluvio',
      mapping: {
        date: {
          type: 'plain',
          sourceField: 'gg/mm/aaaa',
        },
        'value [mm]': {
          type: 'plain',
          sourceField: 'Precipitazione [mm]',
        },
        type: {
          type: 'plain',
          sourceField: 'Tipo Dato',
        },
      }
    },
    target: {
      config: {
        mongodbUrl: 'mongodb://localhost:27017',
        dbName: 'test',
        collectionName: 'stations'
      }
    }
  }

  const { url } = config.source
  const parsedUrl = new URL(url)
  const { mongodbUrl, dbName, collectionName } = config.target.config
  const sourceMock = nock(parsedUrl.origin)
    .get(`${parsedUrl.pathname}${parsedUrl.search}`)
    .replyWithFile(200, './tests/data/prec_simple.csv', { 'Content-Type': 'text/csv' })

  const initialMongoContent = [
    {
      date: '10/06/1994',
      'value [mm]': '0,4',
      type: 'V'
    },
  ]
  
  const mongoClient = await setupMongoCollection(mongodbUrl, dbName, collectionName, initialMongoContent)
  await start(config)

  const expectedMongoContent = [
    {
      date: '10/06/1994',
      'value [mm]': '0,4',
      type: 'V'
    },
    {
      date: '11/06/1994',
      'value [mm]': '2,4',
      type: 'V'
    },
    {
      date: '12/06/1994',
      'value [mm]': '21,0',
      type: 'V'
    }
  ]
  const actualMongoContent = (await mongoClient.db(dbName).collection(collectionName).find().toArray())
  // eslint-disable-next-line no-unused-vars
    .map(({ _id, ...fields }) => fields)
  
  test.strictSame(actualMongoContent, expectedMongoContent)
  test.ok(sourceMock.isDone())

  await mongoClient.close()
})
tap.test('simple scraping with field filter from csv to mongo', async(test) => {
  const config = {
    source: {
      url: 'http://www.sir.toscana.it/archivio/download.php?IDST=pluvio',
      mapping: {
        date: {
          type: 'plain',
          sourceField: 'gg/mm/aaaa',
        },
        'value [mm]': {
          type: 'plain',
          sourceField: 'Precipitazione [mm]',
        },
      }
    },
    target: {
      config: {
        mongodbUrl: 'mongodb://localhost:27017',
        dbName: 'test',
        collectionName: 'stations'
      }
    }
  }

  const { url } = config.source
  const parsedUrl = new URL(url)
  const { mongodbUrl, dbName, collectionName } = config.target.config
  const sourceMock = nock(parsedUrl.origin)
    .get(`${parsedUrl.pathname}${parsedUrl.search}`)
    .replyWithFile(200, './tests/data/prec_simple.csv', { 'Content-Type': 'text/csv' })

  const initialMongoContent = [
    {
      date: '10/06/1994',
      'value [mm]': '0,4',
    },
  ]
  
  const mongoClient = await setupMongoCollection(mongodbUrl, dbName, collectionName, initialMongoContent)
  await start(config)

  const expectedMongoContent = [
    {
      date: '10/06/1994',
      'value [mm]': '0,4',
    },
    {
      date: '11/06/1994',
      'value [mm]': '2,4',
    },
    {
      date: '12/06/1994',
      'value [mm]': '21,0',
    }
  ]
  const actualMongoContent = (await mongoClient.db(dbName).collection(collectionName).find().toArray())
  // eslint-disable-next-line no-unused-vars
    .map(({ _id, ...fields }) => fields)
  
  test.strictSame(actualMongoContent, expectedMongoContent)
  test.ok(sourceMock.isDone())

  await mongoClient.close()
})

// tap.test('simple scraping from csv to mongo', async(test) => {
//   const config = {
//     source: {
//       url: 'http://www.sir.toscana.it/archivio/download.php?IDST=pluvio',
//       mapping: {
//         stationId: {
//           type: 'plain',
//           sourceField: 'IDStazione',
//         },
//         city: {
//           type: 'plain',
//           sourceField: 'Comune',
//         },
//         province: {
//           type: 'plain',
//           sourceField: 'Provincia',
//         },
//         measurements: {
//           type: 'array',
//           forEach: {
//             sourceField: 'IDStazione',
//             source: {
//               url: 'http://www.sir.toscana.it/archivio/download.php?IDST=pluvio&IDS={{IDStazione}}',
//             }
//             mapping: {
//               date: 
//             }
//           }
//         }
//       }
//     },
//     target: {
//       config: {
//         mongodbUrl: 'mongodb://localhost:27017',
//         dbName: 'test',
//         collectionName: 'stations'
//       }
//     }
//   }

//   const { url } = config.source
//   const parsedUrl = new URL(url)
//   const { mongodbUrl, dbName, collectionName } = config.target.config
//   const sourceMock = nock(parsedUrl.origin)
//     .get(`${parsedUrl.pathname}${parsedUrl.search}`)
//     .replyWithFile(200, './tests/data/prec_simple.csv', { 'Content-Type': 'text/csv' })

//   const initialMongoContent = [
//     {
//       'gg/mm/aaaa': '10/06/1994',
//       'Precipitazione [mm]': '0,4',
//       'Tipo Dato': 'V'
//     },
//   ]
  
//   const mongoClient = await setupMongoCollection(mongodbUrl, dbName, collectionName, initialMongoContent)
//   await start(config)

//   const expectedMongoContent = [
//     {
//       'gg/mm/aaaa': '10/06/1994',
//       'Precipitazione [mm]': '0,4',
//       'Tipo Dato': 'V'
//     },
//     {
//       'gg/mm/aaaa': '11/06/1994',
//       'Precipitazione [mm]': '2,4',
//       'Tipo Dato': 'V'
//     },
//     {
//       'gg/mm/aaaa': '12/06/1994',
//       'Precipitazione [mm]': '21,0',
//       'Tipo Dato': 'V'
//     }
//   ]
//   const actualMongoContent = (await mongoClient.db(dbName).collection(collectionName).find().toArray())
//   // eslint-disable-next-line no-unused-vars
//     .map(({ _id, ...fields }) => fields)
  
//   test.strictSame(actualMongoContent, expectedMongoContent)
//   test.ok(sourceMock.isDone())

//   await mongoClient.close()
// })