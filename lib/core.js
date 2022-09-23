'use strict'

const got = require('got')
const csv2json = require('csvtojson')
const { MongoClient } = require('mongodb')

const start = async (config) => {
  const { url, mapping } = config.source
  const { body: sourceResponse } = await got.get(url)
  const jsonContent = await csv2json({ 
    delimiter: ';',
  }).fromString(sourceResponse)
  const mappedData = mapScrapedData(jsonContent, mapping)
  await writeToMongoDB(config, mappedData)
}

const mapScrapedData = (jsonRawContent, mapping) => {
  if (!mapping) {
    return jsonRawContent
  }
  return jsonRawContent.map(jsonObject => {
    return Object.keys(mapping).reduce((mappedObject, currentKey) => {
      mappedObject[currentKey] = jsonObject[mapping[currentKey].sourceField]
      return mappedObject
    }, {})
  })
}

const writeToMongoDB = async (config, jsonContent) => {
  const { mongodbUrl, dbName, collectionName } = config.target.config
  const client = new MongoClient(mongodbUrl)
  await client.connect()
  const db = client.db(dbName)
  const collection = db.collection(collectionName)
  await collection.insertMany(jsonContent)
  await client.close()
}

module.exports = {
  start
}