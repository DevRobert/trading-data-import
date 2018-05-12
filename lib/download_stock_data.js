
const http = require('http')
const csv = require('csv-parser')
const fs = require('fs')

const downloadUrlTemplate = 'http://www.netfonds.no/quotes/paperhistory.php?paper=#code#.BTSE&csv_format=csv'

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

module.exports = {
    downloadStockData
}
