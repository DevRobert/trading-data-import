const csv = require('csv-parser')
const fs = require('fs')
const MongoClient = require('mongodb').MongoClient
const http = require('http')

const databaseUrl = 'mongodb://localhost:27017'
const databaseName = 'trading'

const downloadUrlTemplate = 'http://www.netfonds.no/quotes/paperhistory.php?paper=#code#.BTSE&csv_format=csv'

let stocks = [
    ['E-ADSD', 'DE000A1EWWW0', 'adidas'],
    ['E-ALVD', 'DE0008404005', 'Allianz'],
    ['E-BASD', 'DE000BASF111', 'BASF'],
    ['E-BAYND', 'DE000BAY0017', 'Bayer'],
    ['E-BEID', 'DE0005200000', 'Beiersdorf'],
    ['E-BMWD', 'DE0005190003', 'BMW'],
    ['E-CBKD', 'DE000CBK1001', 'Commerzbank'],
    ['E-COND', 'DE0005439004', 'Continental'],
    ['E-DAID', 'DE0007100000', 'Daimler'],
    ['E-DBKD', 'DE0005140008', 'Deutsche Bank'],
    ['E-DB1D', 'DE0005810055', 'Deutsche Börse'],
    ['E-DPWD', 'DE0005552004', 'Deutsche Post'],
    ['E-DTED', 'DE0005557508', 'Deutsche Telekom'],
    ['E-EOAND', 'DE000ENAG999', 'E.ON'],
    ['E-FRED', 'DE0005785604', 'Fresenius'],
    ['E-FMED', 'DE0005785802', 'Fresenius Medical Care'],
    ['E-HEID', 'DE0006047004', 'HeidelbergCement'],
    ['E-HEND', 'DE0006048432', 'Henkel vz.'],
    ['E-IFXD', 'DE0006231004', 'Infineon'],
    ['E-LINUD', 'DE000A2E4L75', 'Linde'],
    ['E-LHAD', 'DE0008232125', 'Lufthansa'],
    ['E-MRKD', 'DE0006599905', 'Merck'],
    ['E-MUV2D', 'DE0008430026', 'Münchener Rückversicherungs-Gesellschaft'],
    ['E-PSMD', 'DE000PSM7770', 'ProSiebenSat.1 Media'],
    ['E-RWED', 'DE0007037129', 'RWE'],
    ['E-SAPD', 'DE0007164600', 'SAP'],
    ['E-SIED', 'DE0007236101', 'Siemens'],
    ['E-TKAD', 'DE0007500001', 'thyssenkrupp'],
    ['E-VOW3D', 'DE0007664039', 'Volkswagen (VW) vz.'],
    ['E-VNAD', 'DE000A1ML7J1', 'Vonovia']
]

stocks = stocks.map((stock) => {
    return {
        code: stock[0],
        isin: stock[1],
        name: stock[2]
    }
})

const transformRow = (data, stock) => {
    const shortDateString = data.quote_date
    const parseDateString = shortDateString.substring(0, 4) + '-' + shortDateString.substring(4, 6) + '-' + shortDateString.substring(6, 8)
    const date = new Date(parseDateString)

    const insertData = {
        date
    }

    insertData.stocks = {};

    insertData.stocks[stock.isin] = {
        open: parseFloat(data.open),
        high: parseFloat(data.high),
        low: parseFloat(data.low),
        close: parseFloat(data.close),
        volume: parseFloat(data.volume),
        value: parseFloat(data.value)
    }

    return insertData
}

const downloadStockData = stock => {
    return new Promise((resolve, reject) => {
        const downloadUrl = downloadUrlTemplate.replace(/#code#/, stock.code)
        console.log('Importing quotes for ' + stock.isin + ' from ' + downloadUrl + '...')

        const rows = []

        const pipe = http.get(downloadUrl, response => {
            const pipe = response.pipe(csv())

            pipe.on('data', data => {
                const transformedData = transformRow(data, stock)
                rows.push(transformedData)
            })
    
            pipe.on('end', () => {
                console.log(rows.length + ' quotes imported.')

                resolve(rows)
            })
        })
    })
}

let run = async () => {
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
