const fs = require('fs')
const readline = require('readline');


function readFileByLine(path) {
    let readStream = fs.createReadStream(path, {encoding: 'utf-8'});

    return readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
    })
}

module.exports = readFileByLine