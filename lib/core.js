'use strict'

const got = require('got')
const csv2json = require('csvtojson')
const { MongoClient } = require('mongodb')


const start = async (config) => {
  const { url } = config.source
  const { body: sourceResponse } = await got.get(url)
  const jsonContent = await csv2json({ delimiter: ';' }).fromString(sourceResponse)
  await writeToMongoDB(config, jsonContent)
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