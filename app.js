const _ = require('lodash');
const fs = require('fs');
const {get, write, arrayToBuffer, bufferForEach, getColumn, bufferToArray, clearCache} = require('./util');
const parse = require('./parser');
const optimize = require('./optimizer');
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size * 4;
// 6000000 can pass small
// still need to pause the stream for fs await to work

const MAX_ROW = 2400000;

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

function buildAll(line) {
    //build index
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
    if (fs.statSync(path).size < 161732673) {
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
        metaData.unique = uniqueArray.map((set, index) => set.size || (maxArray[index] - minArray[index]));
        wlArray.length && wlArray.forEach((wl, index) => {
            wl.end(bufArray[index].slice(0, bufferIndexArray[index]), 'binary', () => {
                columnFinishCount++;
                if (columnFinishCount === columnNumber) {
                    buildCount++;
                    if (buildCount === buildCountTotal) {
                        builtFlag = true;
                        // query(`SELECT SUM(A.c40), SUM(E.c4), SUM(D.c1)
                        //        FROM A, C, D, E
                        //        WHERE C.c1 = E.c0 AND A.c2 = C.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
                        //          AND D.c3 > -7349;`, 0)
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
    let {joins, tables, tableIndex, filterByTable, useSituation, accIndex} = optimize(select, from, where, filter, metaDict);
    let result = select.map(() => 0);
    //console.log(select, accIndex)
    select = select.map(([table, col]) => {
        return accIndex[table][col + '']
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
        //console.log(queryNo, result)
        if (total === 0) {
            queryResult.forEach((value) => {
                process.stdout.write(value + '\n')
            });
            process.exit()
        }
        nextQuery();
    });

    async function next(joinNum, acc) {
        if (joinNum < joins.length) {
            return join(joins[joinNum], acc, joinNum)
        }
    }

    async function join([rel, joinTable, allJoin, cutleft, cutright, accIndex], acc, joinNum) {
        //console.log(cutleft,cutright)
        async function pipe(data) {
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
                        columns.push(accIndex[tableName][column]);
                        columns2.push(tableIndex[tableName2][column2])
                    }
                    let db1 = _.groupBy(acc, (row) => {
                        return _(columns).map((column) => getColumn(row, column)).join(',')
                    });
                    acc = [];
                    get(joinTable, tables[joinTable], async (value, index) => {
                        // if found the target, we just store the relationship we need
                        let target = db1[_(columns2).map((column) => getColumn(value, column)).join(',')];
                        if (target) {
                            if (lastFlag) {
                                let len = target.length;
                                for (let i = 0; i < len; i++) {
                                    let row1 = target[i];
                                    let length = row1.length / 4;
                                    let len2 = select.length;
                                    for (let j = 0; j < len2; j++) {
                                        let col = select[j];
                                        if (col >= length) {
                                            result[j] += getColumn(value, col - length + cutright)
                                        } else {
                                            result[j] += getColumn(row1, col)
                                        }
                                    }
                                }

                            } else {
                                for (let i = 0; i < target.length; i++) {
                                    let row1 = target[i];
                                    let cur = Buffer.allocUnsafe(row1.length + value.length - cutright * 4);
                                    row1.copy(cur);
                                    value.copy(cur, row1.length, cutright * 4);
                                    acc.push(cur);
                                    if (acc.length > MAX_ROW) {
                                        let data = acc;
                                        acc = [];
                                        await pipe(data);
                                    }
                                }
                            }
                        }
                    }, inMemoryDataBase, async () => {
                        resolve(pipe(acc))
                    }, useSituation, filterByTable[joinTable])
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
                        columns.push(tableIndex[tableName][column]);
                        columns2.push(tableIndex[tableName2][column2])
                    }


                    let db1 = new Map();
                    acc = [];
                    get(tableName, tables[tableName], (value, index) => {
                        let val = _(columns).map((column) => getColumn(value, column)).join(',');
                        let list = db1.get(val) || [];
                        list.push(value);
                        db1.set(val, list)
                    }, inMemoryDataBase, () => {
                        get(tableName2, tables[tableName2], async (value, index) => {
                            let target = db1.get(_(columns2).map((column) => getColumn(value, column)).join(','));
                            if (target) {
                                if (lastFlag) {
                                    let len = target.length;
                                    for (let i = 0; i < len; i++) {
                                        let row1 = target[i];
                                        let length = row1.length / 4;
                                        let len2 = select.length;
                                        for (let j = 0; j < len2; j++) {
                                            let col = select[j];
                                            if (col >= length) {
                                                result[j] += getColumn(value, col - length + cutright)
                                            } else {
                                                result[j] += getColumn(row1, col)
                                            }
                                        }
                                    }
                                } else {
                                    for (let i = 0; i < target.length; i++) {
                                        let row1 = target[i];
                                        let left = cutleft * 4;
                                        let len1 = row1.length - left
                                        let right = cutright * 4;
                                        let cur = Buffer.allocUnsafe(row1.length + len1 - right);
                                        row1.copy(cur, 0, left);
                                        value.copy(cur, len1, right);
                                        acc.push(cur);
                                        if (acc.length > MAX_ROW) {
                                            let data = acc1;
                                            acc = [];

                                            await pipe(data);
                                        }
                                    }
                                }

                            }// if no same drop
                        }, inMemoryDataBase, async () => {
                            resolve(pipe(acc))
                        }, useSituation, filterByTable[tableName2])
                    }, useSituation, filterByTable[tableName])
                })
            }
        } else {
            let {tableName, tableName2, column, column2} = allJoin;
            if (rel.length > 1) {
                return new Promise((resolve => {
                    //console.log(accIndex,tableName,column,acc[0].length)
                    column = accIndex[tableName][column];
                    column2 = tableIndex[tableName2][column2];
                    let db1 = _.groupBy(acc, (row) => {
                        return getColumn(row, column)
                    });
                    acc = [];
                    get(tableName2, tables[tableName2], async (value, index) => {
                            let target = db1[getColumn(value, column2)];
                            if (target) {
                                if (lastFlag) {
                                    let len = target.length;
                                    for (let i = 0; i < len; i++) {
                                        let row1 = target[i];
                                        let length = row1.length / 4;
                                        let len2 = select.length;
                                        for (let j = 0; j < len2; j++) {
                                            let col = select[j];
                                            if (col >= length) {
                                                result[j] += getColumn(value, col - length + cutright)
                                            } else {
                                                result[j] += getColumn(row1, col)
                                            }
                                        }
                                    }
                                } else {
                                    for (let i = 0; i < target.length; i++) {
                                        let row1 = target[i];
                                        let right = cutright * 4;
                                        let cur = Buffer.allocUnsafe(row1.length + value.length - right);
                                        row1.copy(cur);
                                        value.copy(cur, row1.length, right);
                                        acc.push(cur);
                                        if (acc.length > MAX_ROW) {
                                            let data = acc;
                                            acc = [];
                                            await pipe(data);
                                        }
                                    }
                                }
                            }// if no same drop
                        }
                        ,
                        inMemoryDataBase, async () => {
                            resolve(pipe(acc))
                        }, useSituation, filterByTable[tableName2])
                }))
            } else {
                return new Promise(resolve => {

                    //change column name to its actual position in a row
                    column = tableIndex[tableName][column];
                    column2 = tableIndex[tableName2][column2];

                    let db1 = new Map();
                    acc = [];
                    get(tableName, tables[tableName], (value, index) => {
                        let val = getColumn(value, column);
                        let list = db1.get(val) || [];
                        list.push(value);
                        db1.set(val, list)
                    }, inMemoryDataBase, () => {
                        get(tableName2, tables[tableName2], async (value, index) => {
                            // if found the target, we just store the relationship we need
                            let target = db1.get(getColumn(value, column2));
                            if (target) {
                                if (lastFlag) {
                                    let len = target.length;
                                    for (let i = 0; i < len; i++) {
                                        let row1 = target[i];
                                        let length = row1.length / 4;
                                        let len2 = select.length;
                                        for (let j = 0; j < len2; j++) {
                                            let col = select[j];
                                            if (col >= length) {
                                                result[j] += getColumn(value, col - length + cutright)
                                            } else {
                                                result[j] += getColumn(row1, col)
                                            }
                                        }
                                    }
                                } else {
                                    for (let i = 0; i < target.length; i++) {
                                        let row1 = target[i];
                                        let left = cutleft * 4;
                                        let right = cutright * 4;
                                        let len1 = row1.length - left;
                                        let cur = Buffer.allocUnsafe(len1 + value.length - right);
                                        row1.copy(cur, 0, left);
                                        value.copy(cur, len1, right);
                                        acc.push(cur);
                                        if (acc.length > MAX_ROW) {
                                            let data = acc;
                                            acc = [];
                                            await pipe(data);
                                        }
                                    }
                                }
                            }
                        }, inMemoryDataBase, async () => {
                            resolve(pipe(acc))
                        }, useSituation, filterByTable[tableName2])
                    }, useSituation, filterByTable[tableName])
                })
            }
        }
    }
}