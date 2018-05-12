const { getStocks } = require('./lib/mysql_client')
const { downloadStockData } = require('./lib/download_stock_data')

const MongoClient = require('mongodb').MongoClient

const databaseUrl = 'mongodb://localhost:27017'
const databaseName = 'trading'

let run = async () => {
    const stocks = await getStocks()

    const client = await MongoClient.connect(databaseUrl)

    try {
        const database = client.db(databaseName)
        const collection = database.collection('single-quotes')

        console.log('Clearing quotes...')
        await collection.deleteMany({})
        console.log('Quotes cleared.')

        const downloadPromises = []

        for(stockIndex = 0; stockIndex < stocks.length; stockIndex++) {
            const stock = stocks[stockIndex]
            const document = await downloadStockData(stock)
            await collection.insert(document)
        }
    }
    finally {
        await client.close()
    }
}

console.log('Importing quotes...')

run().then(() => {
    console.log('Import quotes done.')
}).catch((error) => {
    console.error('Import quotes failed.')
    console.error(error)
    process.exit(1)
})
