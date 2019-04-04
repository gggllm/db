const fs = require('fs');
const readline = require('readline');


function readFileByLine(path) {
    let readStream = fs.createReadStream(path, {encoding: 'ASCII'});

    return readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
    })
}

module.exports = readFileByLine;