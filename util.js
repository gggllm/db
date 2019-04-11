const _ = require("lodash");
const fs = require('fs');

let cache = {};

function readFromFile(table, col) {
    let path = `./test/${table}${col}.bin`;
    //console.log(fs.statSync(path).size)
    return fs.createReadStream(path)
}

// cb is for streaming purpose
// cb2 is for running sequentially

async function get(table, colums, cb, inMemoryDataBase, cb2, useSituation, filters = []) {
    colums=[...colums]
    // analyze filter
    let removedColumn = _(filters).filter(([column, filter]) => useSituation[table + column] === 1).groupBy(0).value();
    let filterColumn = _.groupBy(filters, 0)
    let finalColumns = colums.filter((column) => !removedColumn[column]);
    let filterColumns = colums.filter((column) => filterColumn[column]);
    console.log(filterColumn, finalColumns, filterColumns)

    if (cache[table]) {
        let db = cache[table];
        let length = db.length;
        for (let i = 0; i < length; i++) {
            await cb(db[i], i)
        }
        cb2 && cb2()
    } else if (inMemoryDataBase[table]) {
        //console.log(`search ${table} in memory`);
        let db = inMemoryDataBase[table];
        let length = db.length;
        let ch = [];
        for (let i = 0; i < length; i++) {
            let row = db[i];
            let flag = false;
            for (let i in filters) {
                let [column, filter] = filters[i];
                if (!filter(getColumn(row, column))) {
                    flag = true;
                    break
                }
            }
            if (flag) {
                continue
            }
            let length2 = finalColumns.length;
            let res = Buffer.allocUnsafe(length2 * 4);
            for (let i = 0; i < length2; i++) {
                res.writeInt32LE(getColumn(row, finalColumns[i]), i * 4)
            }
            await cb(res, i);
            ch.push(res)
        }
        cache[table] = ch;
        cb2 && cb2()
    } else {
        //console.log(`search ${table} in disk`);
        let db = [];
        let sizeArray = [];
        let finished = 0;
        let drop = [];
        let columnNumber = filterColumns.length + finalColumns.length;
        let ch = [];
        filters = _.groupBy(filters, 0);
        filterColumns.forEach((col, index) => {
            let rl = readFromFile(table, col);
            let rowNumber = 0;
            let colFilters = _.map(filters[col], 1);
            let lastChunk;
            rl.on('data', async (chunk) => {
                if (lastChunk && lastChunk.length !== 0) {
                    chunk = Buffer.concat([lastChunk, chunk])
                }
                let cursor = 0;
                let value;
                while (cursor + 4 <= chunk.length) {
                    if (drop[rowNumber]) {
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
                        db[rowNumber] = null;// delete this row
                        drop[rowNumber] = true;
                        rowNumber++;
                        continue
                    }
                    let size = sizeArray[rowNumber] || 0;
                    sizeArray[rowNumber] = ++size;
                    if (size === columnNumber) {
                        let row = db[rowNumber];
                        await cb(row);
                        db[rowNumber] = null //delete the finished row
                    }
                    rowNumber++;
                }
                lastChunk = chunk.slice(cursor)
            });
            rl.on('end', () => {
                finished++;
                if (finished === columnNumber) {
                    cb2 && cb2()
                }
            })
        });
        finalColumns.forEach((col, index) => {
            let rl = readFromFile(table, col);
            let rowNumber = 0;
            let lastChunk;
            rl.on('data', async (chunk) => {
                if (lastChunk && lastChunk.length !== 0) {
                    chunk = Buffer.concat([lastChunk, chunk])
                }
                let cursor = 0;
                let value;
                while (cursor + 4 <= chunk.length) {
                    if (drop[rowNumber]) {
                        cursor += 4;
                        rowNumber++;
                        continue
                    }
                    value = chunk.readInt32LE(cursor);
                    cursor += 4;
                    let row = db[rowNumber] || Buffer.allocUnsafe(finalColumns.length * 4);
                    db[rowNumber] = row;
                    let size = sizeArray[rowNumber] || 0;
                    sizeArray[rowNumber] = ++size;
                    setColumn(row, index, value);
                    if (size === columnNumber) {
                        await cb(row);
                        db[rowNumber] = null //delete the finished row
                    }
                    rowNumber++;
                }
                lastChunk = chunk.slice(cursor)
            });
            rl.on('end', () => {
                finished++;
                if (finished === columnNumber) {
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
    let res = [];
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

function setColumnBinary(row, column, value, start, end) {
    row.copy(value, column * 4, start, end)
}

function clearCache() {
    cache = {}
}

function bufferForEach(buffer, cb) {
    let index = 0;
    let length = buffer.length;
    while (index < length) {
        cb(buffer.readInt32LE(index * 4), index);
        index++
    }
}

module.exports = {get, write, arrayToBuffer, getColumn, bufferForEach, bufferToArray, clearCache};