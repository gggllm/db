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
const MAX_ROW = 30000000;

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
        build(path, letter[index])
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
    if (fs.statSync(path).size < 50173260) {
        let ds = [];
        inMemoryDataBase[tableName] = ds;
        let cur = [];
        write = (item, index) => {
            cur.push(item);
            minArray[index] = Math.min(minArray[index], item)
            maxArray[index] = Math.max(maxArray[index], item)
            uniqueArray[index].add(item)
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
            minArray[index] = Math.min(minArray[index], item)
            maxArray[index] = Math.max(maxArray[index], item)
            uniqueArray[index].add(item)
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
        metaData.max = maxArray
        metaData.min = minArray
        metaData.unique = uniqueArray.map((set) => set.size)
        //console.log(new Date().getTime() - start)
        wlArray.length && wlArray.forEach((wl, index) => {
            wl.end(bufArray[index].slice(0, bufferIndexArray[index]), 'binary', () => {
                columnFinishCount++;
                if (columnFinishCount === columnNumber) {
                    buildCount++;
                    if (buildCount === buildCountTotal) {
                        //console.log(new Date().getTime() - start)
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
        //console.log(arg[1])
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
    function calculateAccIndex(joins, tables) {
        function addIndex(tableName) {
            let tIndex = {};
            accIndex[tableName] = tIndex;
            let curIndex = tableIndex[tableName];
            for (let col in curIndex) {
                tIndex[col] = accLength + curIndex[col]
            }
            accLength += tables[tableName].length
        }

        let accIndex = {};
        let accLength = 0;
        joins.forEach(({tableName, tableName2, column, column2}) => {
            if (!accIndex[tableName]) {
                addIndex(tableName)
            }
            if (!accIndex[tableName2]) {
                addIndex(tableName2)
            }
        });
        return accIndex
    }

    function addJoin(joined, tableName) {
        joined[tableName] = true
    }

    function isJoined(joined, tableName) {
        return joined[tableName]
    }

    let [select, from, where, filter] = parse(input);
    // get the join sequence and tables that is needed for extraction
    let {joins, tables, tableIndex, filterByTable} = optimize(select, from, where, filter, metaDict);
    let accIndex = calculateAccIndex(_.flatMap(joins, 2), tables);
    //console.log(queryNo)
    //console.log(select, joins, tables, tableIndex, filter)
    let result = select.map(() => {
        return 0
    });
    select = select.map(([table, col]) => {
        return accIndex[table][col]
    });
    next(0).then(() => {// current pipeline is over
        // all data is processed ????
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
        nextQuery();
        if (total === 0) {
            queryResult.forEach((value) => {
                //console.log(value)
                //console.log(acc.length)
                process.stdout.write(value + '\n')
            });
            process.exit()
        }
    });

    async function next(joinNum, acc, joined = {}) {
        if (joinNum < joins.length) {
            return join(joins[joinNum], joined, acc, joinNum)
        }
    }

    async function join([rel, joinTable, allJoin], joined, acc, joinNum) {

        async function pipe(data) {
            if (data.length === 0) {
                return
            }
            return next(joinNum + 1, data, _.clone(joined));
        }

        let lastFlag = false;
        if (joinNum === joins.length - 1) {
            lastFlag = true;
        }

        //console.log(tableName, tableName2)
        // make sure table1 is in the acc
        if (allJoin.push) {
            if (rel.length > 1) {
                return new Promise((resolve => {
                    let columns = []
                    let columns2 = []
                    for (let i = 0; i < allJoin.length; i++) {
                        let {tableName, tableName2, column, column2} = allJoin[i]
                        columns.push(accIndex[tableName][column])
                        // if (tableName !== joinTable) {
                        //     console.log(tableName, joinTable,tableName2)
                        // }
                        columns2.push(tableIndex[tableName2][column2])
                    }
                    addJoin(joined, joinTable);
                    let db1 = _.groupBy(acc, (row) => {
                        return _(columns).map((column) => getColumn(row, column)).join(',')
                    });
                    acc = [];
                    get(joinTable, tables[joinTable], (value, index) => {
                        // if found the target, we just store the relationship we need
                        let target = db1[_(columns2).map((column) => getColumn(value, column)).join(',')];
                        if (target) {
                            target.forEach(async (row1) => {
                                let length = row1.length / 4;
                                if (lastFlag) {
                                    select.forEach((col, index) => {
                                        if (col >= length) {
                                            result[index] += getColumn(value, col - length)
                                        } else {
                                            result[index] += getColumn(row1, col)
                                        }
                                    })
                                } else {
                                    let cur = Buffer.concat([row1, value]);
                                    acc.push(cur);
                                    if (acc.length > MAX_ROW) {
                                        await pipe(acc);
                                        acc = []
                                    }
                                }
                            })
                        }// if no same drop
                    }, inMemoryDataBase, async () => {
                        resolve(pipe(acc))
                    }, filterByTable[joinTable])
                }))
            } else {
                let tableName = rel[0]
                let tableName2 = joinTable
                return new Promise(resolve => {
                    //change column name to its actual position in a row
                    let columns = []
                    let columns2 = []
                    for (let i = 0; i < allJoin.length; i++) {
                        let {tableName, tableName2, column, column2} = allJoin[i]
                        columns.push(tableIndex[tableName][column])
                        columns2.push(tableIndex[tableName2][column2])
                    }


                    let db1 = new Map();
                    acc = [];
                    // calculate the new acc index
                    addJoin(joined, tableName);
                    addJoin(joined, tableName2);

                    get(tableName, tables[tableName], (value, index) => {
                        //console.log(value)
                        let val = _(columns).map((column) => getColumn(value, column)).join(',');
                        let list = db1.get(val) || [];
                        list.push(value);
                        db1.set(val, list)
                    }, inMemoryDataBase, () => {
                        get(tableName2, tables[tableName2], (value, index) => {
                            // if found the target, we just store the relationship we need
                            let target = db1.get(_(columns2).map((column) => getColumn(value, column)).join(','));
                            //console.log(value[column2],target)
                            if (target) {
                                target.forEach(async (row1) => {
                                    let length = row1.length / 4;
                                    if (lastFlag) {
                                        select.forEach((col, index) => {
                                            if (col >= length) {
                                                result[index] += getColumn(value, col - length)
                                            } else {
                                                result[index] += getColumn(row1, col)
                                            }
                                        })
                                    } else {
                                        acc.push(Buffer.concat([row1, value]));
                                        if (acc.length > MAX_ROW) {
                                            await pipe(acc);
                                            acc = []
                                        }
                                    }
                                })
                            }// if no same drop
                        }, inMemoryDataBase, async () => {
                            resolve(pipe(acc))
                        }, filterByTable[tableName2])
                    }, filterByTable[tableName])
                })
            }
        } else {
            let {tableName, tableName2, column, column2} = allJoin
            if (isJoined(joined, tableName)) {
                return new Promise((resolve => {
                    column = accIndex[tableName][column];
                    column2 = tableIndex[tableName2][column2];
                    addJoin(joined, tableName2);
                    let db1 = _.groupBy(acc, (row) => {
                        return getColumn(row, column)
                    });
                    acc = [];
                    get(tableName2, tables[tableName2], (value, index) => {
                        // if found the target, we just store the relationship we need
                        let target = db1[getColumn(value, column2)];
                        if (target) {
                            target.forEach(async (row1) => {
                                let length = row1.length / 4;
                                if (lastFlag) {
                                    select.forEach((col, index) => {
                                        if (col >= length) {
                                            result[index] += getColumn(value, col - length)
                                        } else {
                                            result[index] += getColumn(row1, col)
                                        }
                                    })
                                } else {
                                    let cur = Buffer.concat([row1, value]);
                                    acc.push(cur);
                                    if (acc.length > MAX_ROW) {
                                        await pipe(acc);
                                        acc = []
                                    }
                                }
                            })
                        }// if no same drop
                    }, inMemoryDataBase, async () => {
                        resolve(pipe(acc))
                    }, filterByTable[tableName2])
                }))
            } else {
                return new Promise(resolve => {

                    //change column name to its actual position in a row
                    column = tableIndex[tableName][column];
                    column2 = tableIndex[tableName2][column2];

                    let db1 = new Map();
                    acc = [];
                    // calculate the new acc index
                    addJoin(joined, tableName);
                    addJoin(joined, tableName2);

                    get(tableName, tables[tableName], (value, index) => {
                        //console.log(value)
                        let val = getColumn(value, column);
                        let list = db1.get(val) || [];
                        list.push(value);
                        db1.set(val, list)
                    }, inMemoryDataBase, () => {
                        get(tableName2, tables[tableName2], (value, index) => {
                            // if found the target, we just store the relationship we need
                            let target = db1.get(getColumn(value, column2));
                            //console.log(value[column2],target)
                            if (target) {
                                target.forEach(async (row1) => {
                                    let length = row1.length / 4;
                                    if (lastFlag) {
                                        select.forEach((col, index) => {
                                            if (col >= length) {
                                                result[index] += getColumn(value, col - length)
                                            } else {
                                                result[index] += getColumn(row1, col)
                                            }
                                        })
                                    } else {
                                        acc.push(Buffer.concat([row1, value]));
                                        if (acc.length > MAX_ROW) {
                                            await pipe(acc);
                                            acc = []
                                        }
                                    }
                                })
                            }// if no same drop
                        }, inMemoryDataBase, async () => {
                            resolve(pipe(acc))
                        }, filterByTable[tableName2])
                    }, filterByTable[tableName])
                })
            }
        }
    }
}