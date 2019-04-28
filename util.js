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


 function get(table, colums, cb, inMemoryDataBase, cb2, useSituation, filters = []) {
    // analyze filter
    //colums = [...colums]
    // analyze filter
    let filterColumn = _.groupBy(filters, 0);
    let finalColumns = colums.map((column, index) => column * 4);
    let filterColumns = [];
    //console.log(filters)
    for (let key in filterColumn) {
        filterColumns.push(key)
    }

    // if (cache[table]) {
    //     let db = cache[table];
    //     let length = db.length;
    //     for (let i = 0; i < length; i++) {
    //          cb(db[i], i)
    //     }
    //     cb2 && cb2()
    // } else
    if (inMemoryDataBase[table]) {
        //console.log(`search ${table} in memory`);
        let db = inMemoryDataBase[table];
        let length = db.length;
        //let ch = [];
        filters = filters.map(([column, filter]) => [column * 4, filter])
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
             cb(res, i);
            //ch.push(res)
        }
        //cache[table] = ch;
        cb2 && cb2()
    } else {
        //console.log(`search ${table} in disk`);
        let db = [];
        let sizeArray = [];
        let finished = 0;
        let filterFinished = 0
        let drop = [];
        let columnNumber = finalColumns.length;
        let filterCount = filterColumns.length;
        let ch = [];
        filters = _.groupBy(filters, 0);
        filterColumns.forEach((col, index) => {
            let rl = readFromFile(table, col);
            let rowNumber = 0;
            let colFilters = _.map(filters[col], 1);
            let lastChunk;
            rl.on('data',  (chunk) => {
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
                        drop[rowNumber] = true;
                        rowNumber++;
                        continue
                    }
                    // let size = sizeArray[rowNumber] || 0;
                    // sizeArray[rowNumber] = ++size;
                    // if (size === columnNumber) {
                    //     let row = db[rowNumber]
                    //      cb(row, rowNumber);
                    //     db[rowNumber] = null //delete the finished row
                    // }
                    rowNumber++;
                }
                lastChunk = chunk.slice(cursor)
            });
            rl.on('end',  () => {
                filterFinished++;
                if (filterFinished === filterCount) {
                     getData();
                }
            })
        });
        if (filterColumns.length === 0) {
             getData()
        }
        let bufferSize = finalColumns.length * 4;

        function getData() {
            finalColumns.forEach((col, index) => {
                col = col >> 2
                let rl = readFromFile(table, col);
                let rowNumber = 0;
                let lastChunk;
                rl.on('data',  (chunk) => {
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
                        let row = db[rowNumber] || Buffer.allocUnsafe(bufferSize);
                        db[rowNumber] = row;
                        let size = sizeArray[rowNumber] || 0;
                        sizeArray[rowNumber] = ++size;
                        setColumn(row, index, value);
                        if (size === columnNumber) {
                             cb(row, rowNumber);
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
}

 function getAll(table, colums, cb, inMemoryDataBase, cb2, useSituation, filters = []) {
    // analyze filter
    //colums = [...colums]
    // analyze filter
    let filterColumn = _.groupBy(filters, 0);
    let finalColumns = colums.map((column, index) => column * 4);
    let filterColumns = [];
    //console.log(filters)
    for (let key in filterColumn) {
        filterColumns.push(key)
    }
    if (inMemoryDataBase[table]) {
        let result = []
        //console.log(`search ${table} in memory`);
        let db = inMemoryDataBase[table];
        let length = db.length;
        //let ch = [];
        filters = filters.map(([column, filter]) => [column * 4, filter])
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
            result.push(res)
            //ch.push(res)
        }
        //cache[table] = ch;
        cb && cb(result)
        cb2 && cb2()
    } else {
        // if (cache[table]) {
        //     let db = cache[table];
        //     let length = db.length;
        //     for (let i = 0; i < length; i++) {
        //          cb(db[i], i)
        //     }
        //     cb2 && cb2()
        // } else

        //console.log(`search ${table} in disk`);
        let db = [];
        let sizeArray = [];
        let finished = 0;
        let filterFinished = 0
        let drop = [];
        let columnNumber = finalColumns.length;
        let filterCount = filterColumns.length;
        let ch = [];
        filters = _.groupBy(filters, 0);
        filterColumns.forEach((col, index) => {
            let rl = readFromFile(table, col);
            let rowNumber = 0;
            let colFilters = _.map(filters[col], 1);
            let lastChunk;
            rl.on('data',  (chunk) => {
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
                        drop[rowNumber] = true;
                        rowNumber++;
                        continue
                    }
                    // let size = sizeArray[rowNumber] || 0;
                    // sizeArray[rowNumber] = ++size;
                    // if (size === columnNumber) {
                    //     let row = db[rowNumber]
                    //      cb(row, rowNumber);
                    //     db[rowNumber] = null //delete the finished row
                    // }
                    rowNumber++;
                }
                lastChunk = chunk.slice(cursor)
            });
            rl.on('end',  () => {
                filterFinished++;
                if (filterFinished === filterCount) {
                     getData();
                }
            })
        });
        if (filterColumns.length === 0) {
             getData()
        }
        let bufferSize = finalColumns.length * 4;
        let res = []

        function getData() {
            finalColumns.forEach((col, index) => {
                col = col >> 2
                let rl = readFromFile(table, col);
                let rowNumber = 0;
                let lastChunk;
                rl.on('data',  (chunk) => {
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
                        let row = db[rowNumber] || Buffer.allocUnsafe(bufferSize);
                        db[rowNumber] = row;
                        let size = sizeArray[rowNumber] || 0;
                        sizeArray[rowNumber] = ++size;
                        setColumn(row, index, value);
                        if (size === columnNumber) {
                            res.push(row)
                            db[rowNumber] = null //delete the finished row
                        }
                        rowNumber++;
                    }
                    lastChunk = chunk.slice(cursor)
                });
                rl.on('end', () => {
                    finished++;
                    if (finished === columnNumber) {
                        cb && cb(res)
                        cb2 && cb2()
                    }
                })
            })
        }
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
    return row.readInt32LE(column)
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

module.exports = {get, write, arrayToBuffer, getColumn, bufferForEach, bufferToArray, clearCache, getAll};