const _ = require('lodash');
const fs = require('fs');
const {get, write, arrayToBuffer, bufferForEach, getColumn, bufferToArray, clearCache, sortInMemoryTable} = require('./util');
const parse = require('./parser');
const optimize = require('./optimizer');
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size << 2;
// 6000000 can pass small
// still need to pause the stream for fs await to work


let builtFlag = false;
const letter = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"];

if (!fs.existsSync('./test')) {
    fs.mkdirSync('./test/');
}
let metaDict = {};
let inMemoryDataBase = {};
let buildCount = 0;

let readline = require('readline');
let command = readline.createInterface({
    input: process.stdin,
    terminal: false
});
let lineCount = 0;
let total;
let q = '';
let queryNo = 0;
let queryResult = [];
let buildCountTotal = 0;
let start
function buildAll(line) {
    //build index
    start=new Date()
    let lines = line.split(',');
    buildCountTotal = lines.length;
    lines.forEach((path, index) => {
        build(path, path.charAt(path.indexOf('.csv') - 1))
    })
}

//test
// let paths = []
// for (let i = 0; i < 6; i++) {
//     paths.push(`./pa3_data/data/xxs/${letter[i]}.csv`)
// }
// buildAll(paths.join(','))

command.on('line'
    , function (line) {
        lineCount++;
        if (lineCount === 1) {
            buildAll(line)
        } else if (lineCount === 2) {
            total = parseInt(line);
            for (let i = 0; i < total; i++) {
                queryResult.push('')
            }
        } else {
            if (!line.trim()) {
                q && addQuery(q, queryNo++);
                q = ''
            } else {
                q += line + '\n';
            }

        }
    });


function build(path, tableName) {

    let write;
    let bufArray = [];
    let wlArray = [];
    let bufferIndexArray = [];
    let minArray = [];
    let maxArray = [];
    let uniqueArray = [];
    //261732673
    // 561732673
    // for table smaller then 20mb, store them in memory
    if (fs.statSync(path).size < 20000000) {
        let ds = [];
        inMemoryDataBase[tableName] = ds;
        let cur = [];
        write = (item, index) => {
            cur.push(item);
            minArray[index] = Math.min(minArray[index], item);
            maxArray[index] = Math.max(maxArray[index], item);
            uniqueArray[index].add(item);
            if (index === columnNumber - 1) {
                ds.push(arrayToBuffer(cur));
                cur = [];
            }
        }
        // < 200mb calculate the real size
    } else if (fs.statSync(path).size < 20000000) {
        write = function (item, index) {
            let buf = bufArray[index];
            let wl = wlArray[index];
            let bufferIndex = bufferIndexArray[index];
            buf.writeInt32LE(item, bufferIndex);
            minArray[index] = Math.min(minArray[index], item);
            maxArray[index] = Math.max(maxArray[index], item);
            uniqueArray[index].add(item)
            bufferIndex += 4;
            if (bufferIndex === buffer_size) {
                wl.write(buf, 'binary');
                bufArray[index] = Buffer.allocUnsafe(buffer_size);
                bufferIndex = 0
            }
            bufferIndexArray[index] = bufferIndex
        }
    } else {
        write = function (item, index) {
            let buf = bufArray[index];
            let wl = wlArray[index];
            let bufferIndex = bufferIndexArray[index];
            buf.writeInt32LE(item, bufferIndex);
            minArray[index] = Math.min(minArray[index], item);
            maxArray[index] = Math.max(maxArray[index], item);
            //uniqueArray[index].add(item)
            bufferIndex += 4;
            // can be done using ==== because we manually set it to 4 times
            if (bufferIndex === buffer_size) {
                wl.write(buf, 'binary');
                bufArray[index] = Buffer.allocUnsafe(buffer_size);
                bufferIndex = 0
            }
            bufferIndexArray[index] = bufferIndex
        }
    }

    let rl = readFileByLine(path);

// use buffer to write one block at a time


    let columnNumber = 0;
    let columnFinishCount = 0;
    let lineNumber = 0;
    let metaData = {};
    metaDict[tableName] = metaData;
    rl.on('line', (line) => {
        lineNumber++;
        if (columnNumber === 0) {
            columnNumber = 1;
            minArray.push(2147483647);
            maxArray.push(-2147483648);
            uniqueArray.push(new Set());
            for (let i = 0; i < line.length; i++) {
                if (line.charAt(i) === ',') {
                    columnNumber++;
                    minArray.push(2147483647);
                    maxArray.push(-2147483648);
                    uniqueArray.push(new Set())
                }
            }
            metaData.col = columnNumber;
            for (let i = 0; i < columnNumber; i++) {
                let wl = fs.createWriteStream(`./test/${tableName}${i}.bin`, {encoding: 'binary'});
                wlArray.push(wl);
                bufArray.push(Buffer.allocUnsafe(buffer_size));
                bufferIndexArray.push(0)
            }
        }
        let length = line.length;
        let acc = 0;
        let flag = 1;

        let column = 0;
        for (let i = 0; i < length; i++) {
            let ch = line.charAt(i);
            if (ch === ',') {
                write(acc * flag, column++);
                acc = 0;
                flag = 1
            } else if (ch === '-') {
                flag = -1
            } else {
                acc = acc * 10 + (ch - 0)
            }
        }
        write(acc * flag, column)
    });


    rl.on('close', () => {
        metaData.size = lineNumber;
        metaData.max = maxArray;
        metaData.min = minArray;
        // remove unique calculation to boost
        //metaData.unique = uniqueArray.map((set, index) => set.size || (maxArray[index] - minArray[index]));
        metaData.unique = uniqueArray.map((set, index) => set.size || Math.min(lineNumber, maxArray[index] - minArray[index]));
        wlArray.length && wlArray.forEach((wl, index) => {
            wl.end(bufArray[index].slice(0, bufferIndexArray[index]), 'binary', () => {
                columnFinishCount++;
                if (columnFinishCount === columnNumber) {
                    buildCount++;
                    if (buildCount === buildCountTotal) {
                        builtFlag = true;p
                        // query(`SELECT SUM(A.c40), SUM(E.c4), SUM(D.c1)
                        //        FROM A, C, D, E
                        //        WHERE C.c1 = E.c0 AND A.c2 = C.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
                        //          AND D.c3 > -7349;`, 0)
                        console.error(new Date()-start)
                        nextQuery()
                    }
                }
            })
        })
    });
}

let queryArray = [];
let queryNumber = 0;

function nextQuery() {
    if (queryNumber < queryArray.length) {
        let arg = queryArray[queryNumber++];
        // clear cache to make sure memory is clean
        clearCache();
        query(...arg)
    } else if (total !== 0) {
        setTimeout(() => {
            nextQuery()
        }, 100)
    }
}

function addQuery(input, queryNo) {
    queryArray.push([input, queryNo])
}


function query(input, queryNo) {
    let [select, from, where, filter] = parse(input);
    // get the join sequence and tables that is needed for extraction
    let {joins, tables, tableIndex, filterByTable, useSituation, accIndex} = optimize(select, from, where, filter, metaDict, inMemoryDataBase);
    let result = select.map(() => 0);
    //console.error(select, from, where, filter,joins, tables, tableIndex, filterByTable, useSituation, accIndex)
    select = select.map(([table, col]) => {
        return accIndex[table][col + ''] << 2
    });
    next(0).then(() => {// current pipeline is over
        total--;
        let emptyFlag = true;
        for (let i = 0; i < result.length; i++) {
            if (result[i] !== 0) {
                emptyFlag = false;
                break
            }
        }
        if (emptyFlag) {
            result = result.map(() => '')
        }
        queryResult[queryNo] = result.join(',');
        //console.error(queryNo, result);
        if (total === 0) {
            queryResult.forEach((value) => {
                process.stdout.write(value + '\n')
            });
            process.exit()
        }
        nextQuery();
    });

    function next(joinNum, acc) {
        if (joinNum < joins.length) {
            return join(joins[joinNum], acc, joinNum)
        }
    }

    function join([rel, joinTable, allJoin, cutleft, cutright, accIndex], acc = [], joinNum) {
        //console.log(cutleft,cutright)
        function pipe(data) {
            global.gc && global.gc()
            if (data.length === 0) {
                return
            }
            return next(joinNum + 1, data);
        }

        let lastFlag = false;
        if (joinNum === joins.length - 1) {
            lastFlag = true;
        }

        // make sure all join is an array
        if (allJoin.push) {
            if (rel.length > 1) {
                return new Promise((resolve => {
                    let columns = [];
                    let columns2 = [];
                    for (let i = 0; i < allJoin.length; i++) {
                        let {tableName, tableName2, column, column2} = allJoin[i];
                        columns.push(accIndex[tableName][column] << 2);
                        columns2.push(tableIndex[tableName2][column2] << 2)
                    }
                    let db1 = new Map();
                    for (let index = 0; index < acc.length; index++) {
                        let row = acc[index];
                        acc[index] = null
                        let key = row.readInt32LE(columns[0]);
                        for (let colIndex = 1; colIndex < columns.length; colIndex++) {
                            let column = columns[colIndex];
                            key += ',' + row.readInt32LE(column)
                        }
                        let arr = db1.get(key) || [];
                        db1.set(key, arr);
                        arr.push(row)

                    }
                    acc = [];
                    const right = cutright << 2;
                    if (lastFlag) {
                        get(joinTable, tables[joinTable], (value, index) => {
                            let key = value.readInt32LE(columns2[0]);
                            for (let colIndex = 1; colIndex < columns2.length; colIndex++) {
                                let column = columns2[colIndex];
                                key += ',' + value.readInt32LE(column)
                            }
                            // if found the target, we just store the relationship we need
                            let target = db1.get(key);
                            if (target) {
                                let len = target.length;
                                for (let i = 0; i < len; i++) {
                                    let row1 = target[i];
                                    let length = row1.length;
                                    let len2 = select.length;
                                    for (let j = 0; j < len2; j++) {
                                        let col = select[j];
                                        if (col >= length) {
                                            result[j] += value.readInt32LE(col - length + right)
                                        } else {
                                            result[j] += row1.readInt32LE(col)
                                        }
                                    }
                                }
                            }
                        }, inMemoryDataBase, () => {
                            resolve(pipe(acc))
                        }, useSituation, filterByTable[joinTable])
                    } else {
                        get(joinTable, tables[joinTable], (value, index) => {
                            let key = value.readInt32LE(columns2[0]);
                            for (let colIndex = 1; colIndex < columns2.length; colIndex++) {
                                let column = columns2[colIndex];
                                key += ',' + value.readInt32LE(column)
                            }
                            // if found the target, we just store the relationship we need
                            let target = db1.get(key);
                            if (target) {
                                for (let i = 0; i < target.length; i++) {
                                    let row1 = target[i];
                                    let cur = Buffer.allocUnsafe(row1.length + value.length - right);
                                    row1.copy(cur);
                                    value.copy(cur, row1.length, right);
                                    acc.push(cur);
                                }
                            }
                        }, inMemoryDataBase, () => {
                            resolve(pipe(acc))
                        }, useSituation, filterByTable[joinTable])
                    }
                }))
            } else {
                let tableName = rel[0];
                let tableName2 = joinTable;
                return new Promise(resolve => {
                    //change column name to its actual position in a row
                    let columns = [];
                    let columns2 = [];
                    for (let i = 0; i < allJoin.length; i++) {
                        let {tableName, tableName2, column, column2} = allJoin[i];
                        columns.push(tableIndex[tableName][column] << 2);
                        columns2.push(tableIndex[tableName2][column2] << 2)
                    }


                    let db1 = new Map();
                    acc = [];
                    const left = cutleft << 2;
                    const right = cutright << 2;
                    get(tableName, tables[tableName], (value, index) => {
                        let key = value.readInt32LE(columns[0]);
                        for (let colIndex = 1; colIndex < columns.length; colIndex++) {
                            let column = columns[colIndex];
                            key += ',' + value.readInt32LE(column)
                        }
                        let list = db1.get(key) || [];
                        list.push(value);
                        db1.set(key, list)
                    }, inMemoryDataBase, () => {
                        if (lastFlag) {
                            get(tableName2, tables[tableName2], (value, index) => {
                                let key = value.readInt32LE(columns2[0]);
                                for (let colIndex = 1; colIndex < columns2.length; colIndex++) {
                                    let column = columns2[colIndex];
                                    key += ',' + value.readInt32LE(column)
                                }
                                // if found the target, we just store the relationship we need
                                let target = db1.get(key);
                                if (target) {
                                    let len = target.length;
                                    for (let i = 0; i < len; i++) {
                                        let row1 = target[i];
                                        let length = row1.length;
                                        let len2 = select.length;
                                        for (let j = 0; j < len2; j++) {
                                            let col = select[j];
                                            if (col >= length) {
                                                result[j] += row1.readInt32LE(col - length + right)
                                            } else {
                                                result[j] += row1.readInt32LE(col)
                                            }
                                        }
                                    }
                                }// if no same drop
                            }, inMemoryDataBase, () => {
                                resolve(pipe(acc))
                            }, useSituation, filterByTable[tableName2])
                        } else {
                            get(tableName2, tables[tableName2], (value, index) => {
                                let key = value.readInt32LE(columns2[0]);
                                for (let colIndex = 1; colIndex < columns2.length; colIndex++) {
                                    let column = columns2[colIndex];
                                    key += ',' + value.readInt32LE(column)
                                }
                                // if found the target, we just store the relationship we need
                                let target = db1.get(key);
                                if (target) {
                                    for (let i = 0; i < target.length; i++) {
                                        let row1 = target[i];
                                        let len1 = row1.length - left;
                                        let cur = Buffer.allocUnsafe(row1.length + len1 - right);
                                        row1.copy(cur, 0, left);
                                        value.copy(cur, len1, right);
                                        acc.push(cur);
                                    }
                                }// if no same drop
                            }, inMemoryDataBase, () => {
                                resolve(pipe(acc))
                            }, useSituation, filterByTable[tableName2])
                        }
                    }, useSituation, filterByTable[tableName])
                })
            }
        } else {
            let {tableName, tableName2, column, column2} = allJoin;
            if (rel.length > 1) {
                return new Promise((resolve => {
                    //console.log(accIndex,tableName,column,acc[0].length)
                    column = accIndex[tableName][column] << 2;
                    //console.error(column2)
                    let oriColumn2 = column2 << 2;
                    column2 = tableIndex[tableName2][column2] << 2;
                    let db1 = new Map();
                    for (let index = 0; index < acc.length; index++) {
                        let row = acc[index];
                        acc[index] = null
                        let key = row.readInt32LE(column);
                        let arr = db1.get(key) || [];
                        db1.set(key, arr);
                        arr.push(row)
                    }
                    acc = [];
                    const right = cutright << 2;
                    if (lastFlag) {
                        if (inMemoryDataBase[tableName2]) {
                            let colums = tables[tableName2];
                            let filters = filterByTable[tableName2] || [];
                            let finalColumns = colums.map((column, index) => column << 2);
                            let db = inMemoryDataBase[tableName2];
                            let length = db.length
                            //let ch = [];
                            filters = filters.map(([column, filter]) => [column << 2, filter]);
                            for (let i = 0; i < length; i++) {
                                let row = db[i];
                                let flag = false;
                                for (let i in filters) {
                                    let [column, filter] = filters[i];
                                    if (!filter(row.readInt32LE(column))) {
                                        flag = true;
                                        break
                                    }
                                }
                                if (!db1.has(row.readInt32LE(oriColumn2))) {
                                    continue
                                }
                                if (flag) {
                                    continue
                                }
                                let length2 = finalColumns.length;
                                let value = Buffer.allocUnsafe(length2 << 2);
                                for (let i = 0; i < length2; i++) {
                                    value.writeInt32LE(row.readInt32LE(finalColumns[i]), i << 2)
                                }
                                let target = db1.get(value.readInt32LE(column2));
                                let len = target.length;
                                for (let i = 0; i < len; i++) {
                                    let row1 = target[i];
                                    let length = row1.length;
                                    let len2 = select.length;
                                    for (let j = 0; j < len2; j++) {
                                        let col = select[j];
                                        if (col >= length) {
                                            result[j] += value.readInt32LE(col - length + right)
                                        } else {
                                            result[j] += row1.readInt32LE(col)
                                        }
                                    }
                                }
                            }
                            resolve(pipe(acc))
                        } else {
                            get(tableName2, tables[tableName2], (value, index) => {
                                    let target = db1.get(value.readInt32LE(column2));
                                    if (target) {
                                        let len = target.length;
                                        for (let i = 0; i < len; i++) {
                                            let row1 = target[i];
                                            let length = row1.length;
                                            let len2 = select.length;
                                            for (let j = 0; j < len2; j++) {
                                                let col = select[j];
                                                if (col >= length) {
                                                    result[j] += value.readInt32LE(col - length + right)
                                                } else {
                                                    result[j] += row1.readInt32LE(col)
                                                }
                                            }
                                        }
                                    }// if no same drop
                                },
                                inMemoryDataBase, () => {
                                    resolve(pipe(acc))
                                }, useSituation, filterByTable[tableName2])
                        }
                    } else {
                        if (inMemoryDataBase[tableName2]) {
                            let colums = tables[tableName2];
                            let filters = filterByTable[tableName2] || [];
                            let finalColumns = colums.map((column, index) => column << 2);
                            let db = inMemoryDataBase[tableName2];
                            let length = db.length
                            //let ch = [];
                            filters = filters.map(([column, filter]) => [column << 2, filter]);
                            for (let i = 0; i < length; i++) {
                                let row = db[i];
                                let flag = false;
                                for (let i in filters) {
                                    let [column, filter] = filters[i];
                                    if (!filter(row.readInt32LE(column))) {
                                        flag = true;
                                        break
                                    }
                                }
                                if (!db1.has(row.readInt32LE(oriColumn2))) {
                                    continue
                                }
                                if (flag) {
                                    continue
                                }
                                let length2 = finalColumns.length;
                                let value = Buffer.allocUnsafe(length2 << 2);
                                for (let i = 0; i < length2; i++) {
                                    value.writeInt32LE(row.readInt32LE(finalColumns[i]), i << 2)
                                }
                                let target = db1.get(value.readInt32LE(column2));
                                for (let i = 0; i < target.length; i++) {
                                    let row1 = target[i];
                                    let cur = Buffer.allocUnsafe(row1.length + value.length - right);
                                    row1.copy(cur);
                                    value.copy(cur, row1.length, right);
                                    acc.push(cur);
                                }
                            }
                            resolve(pipe(acc))
                        } else {
                            get(tableName2, tables[tableName2], (value, index) => {
                                    let target = db1.get(value.readInt32LE(column2));
                                    if (target) {
                                        for (let i = 0; i < target.length; i++) {
                                            let row1 = target[i];
                                            let cur = Buffer.allocUnsafe(row1.length + value.length - right);
                                            row1.copy(cur);
                                            value.copy(cur, row1.length, right);
                                            acc.push(cur);
                                        }
                                    }// if no same drop
                                },
                                inMemoryDataBase, () => {
                                    resolve(pipe(acc))
                                }, useSituation, filterByTable[tableName2])
                        }
                    }
                }))
            } else {
                return new Promise(resolve => {

                    //change column name to its actual position in a row
                    let oriColumn = column << 2;
                    let oriColumn2 = column2 << 2;
                    column = tableIndex[tableName][column] << 2;
                    column2 = tableIndex[tableName2][column2] << 2;

                    let db1 = new Map();
                    acc = [];
                    const left = cutleft << 2;
                    const right = cutright << 2;
                    get(tableName, tables[tableName], (value, index) => {
                        let val = value.readInt32LE(column);
                        let list = db1.get(val) || [];
                        list.push(value);
                        db1.set(val, list)
                    }, inMemoryDataBase, () => {
                        if (lastFlag) {
                            if (inMemoryDataBase[tableName2]) {
                                let colums = tables[tableName2];
                                let filters = filterByTable[tableName2] || [];
                                let finalColumns = colums.map((column, index) => column << 2);
                                let db = inMemoryDataBase[tableName2];
                                let length = db.length;
                                //let ch = [];
                                filters = filters.map(([column, filter]) => [column << 2, filter]);
                                for (let i = 0; i < length; i++) {
                                    let row = db[i];
                                    let flag = false;
                                    for (let i in filters) {
                                        let [column, filter] = filters[i];
                                        if (!filter(row.readInt32LE(column))) {
                                            flag = true;
                                            break
                                        }
                                    }
                                    if (!db1.has(row.readInt32LE(oriColumn2))) {
                                        continue
                                    }
                                    if (flag) {
                                        continue
                                    }
                                    let length2 = finalColumns.length;
                                    let value = Buffer.allocUnsafe(length2 << 2);
                                    for (let i = 0; i < length2; i++) {
                                        value.writeInt32LE(row.readInt32LE(finalColumns[i]), i << 2)
                                    }
                                    let target = db1.get(value.readInt32LE(column2));
                                    let len = target.length;
                                    for (let i = 0; i < len; i++) {
                                        let row1 = target[i];
                                        let length = row1.length;
                                        let len2 = select.length;
                                        for (let j = 0; j < len2; j++) {
                                            let col = select[j];
                                            if (col >= length) {
                                                result[j] += value.readInt32LE(col - length + right)
                                            } else {
                                                result[j] += row1.readInt32LE(col)
                                            }
                                        }
                                    }
                                }
                                resolve(pipe(acc))
                            } else {
                                get(tableName2, tables[tableName2], (value, index) => {
                                    // if found the target, we just store the relationship we need
                                    let target = db1.get(value.readInt32LE(column2));
                                    if (target) {
                                        let len = target.length;
                                        for (let i = 0; i < len; i++) {
                                            let row1 = target[i];
                                            let length = row1.length;
                                            let len2 = select.length;
                                            for (let j = 0; j < len2; j++) {
                                                let col = select[j];
                                                if (col >= length) {
                                                    result[j] += value.readInt32LE(col - length + right)
                                                } else {
                                                    result[j] += row1.readInt32LE(col)
                                                }
                                            }
                                        }
                                    }
                                }, inMemoryDataBase, () => {
                                    resolve(pipe(acc))
                                }, useSituation, filterByTable[tableName2])
                            }
                        } else {
                            if (inMemoryDataBase[tableName2]) {
                                let colums = tables[tableName2];
                                let filters = filterByTable[tableName2] || [];
                                let finalColumns = colums.map((column, index) => column << 2);
                                let db = inMemoryDataBase[tableName2];
                                let length = db.length;
                                //let ch = [];
                                filters = filters.map(([column, filter]) => [column << 2, filter]);
                                for (let i = 0; i < length; i++) {
                                    let row = db[i];
                                    let flag = false;
                                    for (let i in filters) {
                                        let [column, filter] = filters[i];
                                        if (!filter(row.readInt32LE(column))) {
                                            flag = true;
                                            break
                                        }
                                    }
                                    if (!db1.has(row.readInt32LE(oriColumn2))) {
                                        continue
                                    }
                                    if (flag) {
                                        continue
                                    }
                                    let length2 = finalColumns.length;
                                    let value = Buffer.allocUnsafe(length2 << 2);
                                    for (let i = 0; i < length2; i++) {
                                        value.writeInt32LE(row.readInt32LE(finalColumns[i]), i << 2)
                                    }
                                    let target = db1.get(value.readInt32LE(column2));

                                    for (let i = 0; i < target.length; i++) {
                                        let row1 = target[i];
                                        let len1 = row1.length - left;
                                        let cur = Buffer.allocUnsafe(len1 + value.length - right);
                                        row1.copy(cur, 0, left);
                                        value.copy(cur, len1, right);
                                        acc.push(cur);
                                    }
                                }
                                resolve(pipe(acc))
                            } else {
                                get(tableName2, tables[tableName2], (value, index) => {
                                    // if found the target, we just store the relationship we need
                                    let target = db1.get(value.readInt32LE(column2));
                                    if (target) {

                                        for (let i = 0; i < target.length; i++) {
                                            let row1 = target[i];
                                            let len1 = row1.length - left;
                                            let cur = Buffer.allocUnsafe(len1 + value.length - right);
                                            row1.copy(cur, 0, left);
                                            value.copy(cur, len1, right);
                                            acc.push(cur);
                                        }
                                    }
                                }, inMemoryDataBase, () => {
                                    resolve(pipe(acc))
                                }, useSituation, filterByTable[tableName2])
                            }
                        }
                    }, useSituation, filterByTable[tableName])
                })
            }
        }
    }
}