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
        let db = inMemoryDataBase[table]
        let length = db.length
        for (let i = 0; i < length; i++) {
            let row = db[i]
            let flag = false
            for (let i in filters) {
                let [column, filter] = filters[i]
                if (!filter(getColumn(row, column))) {
                    flag = true
                    break
                }
            }
            if (flag) {
                continue
            }
            let length2 = colums.length
            let res = Buffer.allocUnsafe(length2 * 4)
            for (let i = 0; i < length2; i++) {
                res.writeInt32LE(getColumn(row,colums[i]), i * 4)
            }
            cb(res, i)
        }
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
                    let row = db[rowNumber] || Buffer.allocUnsafe(columnNumber * 4);
                    db[rowNumber] = row;
                    let size = sizeArray[rowNumber] || 0;
                    sizeArray[rowNumber] = ++size;
                    setColumn(row, index, value);
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

function arrayToBuffer(array) {
    let length = array.length;
    let buffer = Buffer.allocUnsafe(length * 4);
    for (let i = 0; i < length; i++) {
        buffer.writeInt32LE(array[i], i * 4)
    }
    return buffer
}

function bufferToArray(buffer) {
    let index = 0;
    let length = buffer.length;
    let res = []
    while (index * 4 < length) {
        res.push(buffer.readInt32LE(index * 4));
        index++
    }
    return res
}

function getColumn(row, column) {
    return row.readInt32LE(column * 4)
}

function setColumn(row, column, value) {
    row.writeInt32LE(value, column * 4)
}

function bufferForEach(buffer, cb) {
    let index = 0;
    let length = buffer.length;
    while (index < length) {
        cb(buffer.readInt32LE(index * 4), index);
        index++
    }
}

module.exports = {get, write, arrayToBuffer, getColumn, bufferForEach, bufferToArray};