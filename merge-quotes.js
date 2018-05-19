const MongoClient = require('mongodb').MongoClient
const Config = require('./config')

async function run() {
    const client = await MongoClient.connect(Config.databaseUrl)

    try {
        const database = client.db(Config.databaseName)
        const collection = database.collection('single-quotes')

        await collection.aggregate([
            { $group: { _id: "$date", stocks: { $mergeObjects: "$stocks" } } },
            { $out: 'merged-quotes' }
        ]).toArray()
    }
    finally {
        await client.close()
    }
}

console.log('Merging quotes...')

run().then(() => {
    console.log('Quotes mergred.')
}).catch((error) => {
    console.error('Merging quotes failed.')
    console.error(error)
    process.exit(1)
})
