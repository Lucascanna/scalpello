'use strict'

const got = require('got')
const csv2json = require('csvtojson')
const { MongoClient } = require('mongodb')

const start = async (config) => {
  const { url, mapping } = config.source
  const { body: sourceResponse } = await got.get(url)
  const jsonContent = await csv2json({ delimiter: ';',})
    .fromString(sourceResponse)
  const mappedData = await mapScrapedData(jsonContent, mapping)
  await writeToMongoDB(config, mappedData)
}

const mapScrapedData = async (jsonRawContent, mapping) => {
  if (!mapping) {
    return jsonRawContent
  }
  const mappedData = []
  for (const jsonRawObject of jsonRawContent) {
    const jsonMappedObject = {}
    for (const currentKey of Object.keys(mapping)) {
      if (mapping[currentKey].type === 'plain') {
        jsonMappedObject[currentKey] = jsonRawObject[mapping[currentKey].sourceField]
      }
      if (mapping[currentKey].type === 'array') {
        const { sourceField, mapping: childMapping, source: { url: urlToInterpolate } } = mapping[currentKey].forEach
        const re = new RegExp(`{{${sourceField}}}`, 'g');
        const interpolatedUrl = urlToInterpolate.replace(re, jsonRawObject[sourceField])
        const { body: sourceResponse } = await got.get(interpolatedUrl)
        const childJsonContent = await csv2json({ delimiter: ';',})
          .fromString(sourceResponse)
        const childMappedData = await mapScrapedData(childJsonContent, childMapping)
        jsonMappedObject[currentKey] = childMappedData
      }
    }
    mappedData.push(jsonMappedObject)
  }
  return mappedData
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