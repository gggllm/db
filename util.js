const _ = require("lodash");
const fs = require('fs');

function readFromFile(table, col) {
    let path = `./test/${table}${col}.bin`
    //console.log(fs.statSync(path).size)
    return fs.createReadStream(path)
}

// cb is for streaming purpose
// cb2 is for running sequentially

function get(table, colums, cb, inMemoryDataBase, cb2, filters = []) {
    if (inMemoryDataBase[table]) {
        //console.log(`search ${table} in memory`);
        _(inMemoryDataBase[table]).filter((row) => {
            for (let i in filters) {
                let [column, filter] = filters[i]
                if (!filter(row[column])) {
                    return false
                }
            }
            return true
        }).map((value) => {
            return colums.map((column => {
                return value[column]
            }))
        }).forEach(cb);
        cb2 && cb2()
    } else {
        //console.log(`search ${table} in disk`);
        let db = [];
        let sizeArray = [];
        let finished = 0;
        let drop = [];
        let columnNumber = colums.length;
        filters = _.groupBy(filters, 0);
        colums.forEach((col, index) => {
            let rl = readFromFile(table, col);
            let rowNumber = 0;
            let colFilters = _.map(filters[col], 1);
            let lastChunk
            rl.on('data', (chunk) => {
                if (lastChunk && lastChunk.length !== 0) {
                    chunk = Buffer.concat([lastChunk, chunk])
                }
                let cursor = 0;
                let value;
                while (cursor + 4 <= chunk.length) {
                    if (drop[rowNumber]) {
                        //console.log('drop')
                        cursor += 4;
                        rowNumber++;
                        continue
                    }
                    value = chunk.readInt32LE(cursor);
                    cursor += 4;
                    let dropFlag = false;
                    for (let filter of colFilters) {
                        if (!filter(value)) {
                            dropFlag = true;
                            break
                        }
                    }
                    if (dropFlag) {
                        //console.log('drop')
                        db[rowNumber] = null;// delete this row
                        drop[rowNumber] = true;
                        rowNumber++;
                        continue
                    }
                    let row = db[rowNumber] || [];
                    db[rowNumber] = row;
                    let size = sizeArray[rowNumber] || 0;
                    sizeArray[rowNumber] = ++size;
                    row[index] = value;
                    if (size === columnNumber) {
                        //console.log(row);
                        cb(row);
                        db[rowNumber] = null //delete the finished row
                    }
                    rowNumber++;
                }
                lastChunk = chunk.slice(cursor)
            });
            rl.on('end', () => {
                finished++;
                if (finished === columnNumber) {
                    //console.log('finished loading data from disk');
                    cb2 && cb2()
                }
            })
        })
    }
}

function write(table, tableName, inMemoryDatabase) {
    inMemoryDatabase[tableName] = table
}


module.exports = {get, write};