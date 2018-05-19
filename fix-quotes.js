const Config = require('./config')
const MongoClient = require('mongodb').MongoClient

console.log('Fixing quotes...')

let previousStocks = {}

const findMissingStocks = (previous, current) => {
    const result = []

    for(var stock in previous) {
        if(!current[stock]) {
            result.push(stock)
        }
    }

    return result
}

const findNewStocks = (previous, current) => {
    const result = []

    for(var stock in current) {
        if(!previous[stock]) {
            result.push(stock)
        }
    }

    return result
}

const processRows = collection => {
    return new Promise((fulfill, reject) => {
        const replaceDocuments = []

        collection.find({}).sort({_id: 1}).forEach(document => {
            const stocks = document.stocks

            const missingStocks = findMissingStocks(previousStocks, stocks)
            const newStocks = findNewStocks(previousStocks, stocks)

            missingStocks.forEach(missingStock => {
                const previousClosePrice = previousStocks[missingStock].close

                stocks[missingStock] = {
                    open: previousClosePrice,
                    high: previousClosePrice,
                    low: previousClosePrice,
                    close: previousClosePrice,
                    volume: 0,
                    value: 0
                }

                replaceDocuments.push(document)

                console.log(document._id.toISOString().substring(0, 10) + ' Missing stock: ' + missingStock + ' Previous close price: ' + previousClosePrice)
            })

            newStocks.forEach(newStock => {
                console.log(document._id.toISOString().substring(0, 10) + ' New stock: ' + newStock)
            })

            previousStocks = stocks
        }, error => {
            if(error) {
                reject(error)
                return
            }

            fulfill(replaceDocuments)
        })
    })
}

const run = async () => {
    const client = await MongoClient.connect(Config.databaseUrl)

    try {
        const database = client.db(Config.databaseName)
        const collection = database.collection('merged-quotes')
        
        const replaceDocuments = await processRows(collection)

        for(let documentIndex = 0; documentIndex < replaceDocuments.length; documentIndex++) {
            const document = replaceDocuments[documentIndex]

            console.log('Document replaced with _id ' + document._id.toISOString().substring(0, 10))

            await collection.replaceOne({
                _id: document._id
            }, document)
        }
    }
    finally {
        await client.close()
    }
}

run().then(() => {
    console.log('Fix quotes done.')
}).catch((error) => {
    console.error('Fix quotes failed.')
    console.error(error)
    process.exit(1)
})
