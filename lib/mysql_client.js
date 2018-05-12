const getStocks = () => {
    return new Promise((fulfill, reject) => {
        const mysql = require('mysql');

        const connection = mysql.createConnection({
            host: 'localhost',
            user: 'root',
            password: 'testtest',
            database: 'trading_production'
        })

        connection.connect()

        connection.query('select isin, name, code from instrument order by isin', function (error, results, fields) {
            if (error) {
                throw error
            }

            connection.end()

            const stocks = results.map(item => {
                return {
                    isin: item.isin,
                    name: item.name,
                    code: item.code
                }
            })

            fulfill(stocks)
        });
    })
}

module.exports = {
    getStocks
}
