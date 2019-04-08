const _ = require('lodash');
const fs = require('fs');
const {get, write} = require('./util');
const parse = require('./parser');
const optimize = require('./optimizer');
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size * 4;

let start = new Date().getTime();

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
command.on('line'
    , function (line) {
        lineCount++;
        if (lineCount === 1) {
            //build index
            line.split(',').forEach((path, index) => {
                build(path, letter[index])
            })
        } else if (lineCount === 2) {
            total = parseInt(line);
            for (let i = 0; i < total; i++) {
                queryResult.push('')
            }
        } else {
            if (!line) {
                q && query(q, queryNo++);
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
    if (fs.statSync(path).size < 501732600) {
        let ds = [];
        inMemoryDataBase[tableName] = ds;
        let index = 0;
        let cur = [];
        write = (item) => {
            cur.push(item);
            index++;
            index = index % columnNumber;
            if (index === 0) {
                ds.push(cur);
                cur = [];
            }
        }
    } else {
        write = function (item, index) {
            let buf = bufArray[index];
            let wl = wlArray[index];
            let bufferIndex = bufferIndexArray[index];
            buf.writeInt32LE(item, bufferIndex);
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
            for (let i = 0; i < line.length; i++) {
                if (line.charAt(i) === ',') {
                    columnNumber++
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
        //console.log(new Date().getTime() - start)
        wlArray.length && wlArray.forEach((wl, index) => {
            wl.end(bufArray[index].slice(0, bufferIndexArray[index]), 'binary', () => {
                columnFinishCount++;
                if (columnFinishCount === columnNumber) {
                    buildCount++;
                    if (buildCount === 16) {
                        //console.log(new Date().getTime() - start)
                    }
                }
            })
        })
    });
}

function query(input, queryNo) {
    let [select, from, where, filter] = parse(input);
    // get the join sequence and tables that is needed for extraction
    let {joins, tables, tableIndex, filterByTable} = optimize(select, from, where, filter, metaDict);
    let acc, accIndex = {}, accLength = 0;
    let joinNum = 0;
    //console.log(select, joins, tables, tableIndex, filterByTable, filter)
    join(joins[joinNum++]);

    function next() {
        if (joinNum < joins.length) {
            //console.log(acc.length, accIndex)
            join(joins[joinNum++])
        } else {
            // do the fucking sum!
            let res = select.map(([table, col]) => {
                let index = accIndex[table][col];
                return _(acc).map(index).sum()
            });
            total--;
            queryResult[queryNo] = res.map((value) => {
                if (value === 0) return '';
                else return value
            }).join(',');
            //console.log(queryNo,res)
            if (total === 0) {
                queryResult.forEach((value) => {
                    //console.log(value)
                    process.stdout.write(value + '\n')
                });
                process.exit()
            }
        }
    }

    function addIndex(tableName) {
        let tIndex = {};
        accIndex[tableName] = tIndex;
        let curIndex = tableIndex[tableName];
        for (let col in curIndex) {
            tIndex[col] = accLength + curIndex[col]
        }
        accLength += tables[tableName].length
    }

    function join({tableName, tableName2, column, column2}) {
        //console.log(tableName, tableName2)
        if (accIndex[tableName] || accIndex[tableName2]) {
            // make sure table1 is in the acc
            if (!accIndex[tableName]) {
                let i = tableName;
                tableName = tableName2;
                tableName2 = i;
                i = column;
                column = column2;
                column2 = i
            }
            column = accIndex[tableName][column];
            column2 = tableIndex[tableName2][column2];
            addIndex(tableName2);
            let db1 = _.groupBy(acc, column);
            acc = [];
            get(tableName2, tables[tableName2], (value, index) => {
                // if found the target, we just store the relationship we need
                let target = db1[value[column2]];
                if (target) {
                    target.forEach((row1) => {
                        acc.push([...row1, ...value])
                    })
                }// if no same drop
            }, inMemoryDataBase, next, filterByTable[tableName2])
        } else {
            // make sure table 1 is smaller then table 2
            // if (metaDict[tableName].size > metaDict[tableName2].size) {
            //     let i = tableName;
            //     tableName = tableName2;
            //     tableName2 = i;
            //     i = column;
            //     column = column2;
            //     column2 = i
            // }
            // change column name to its actual position in a row
            column = tableIndex[tableName][column];
            column2 = tableIndex[tableName2][column2];

            let db1 = new Map();
            acc = [];
            // calculate the new acc index
            addIndex(tableName);
            addIndex(tableName2);

            get(tableName, tables[tableName], (value, index) => {
                //console.log(value)
                let val = value[column];
                let list = db1.get(val) || [];
                list.push(value);
                db1.set(val, list)
            }, inMemoryDataBase, () => {
                get(tableName2, tables[tableName2], (value, index) => {
                    // if found the target, we just store the relationship we need
                    let target = db1.get(value[column2]);
                    //console.log(value[column2],target)
                    if (target) {
                        target.forEach((row1) => {
                            acc.push([...row1, ...value])
                        })
                    }// if no same drop
                }, inMemoryDataBase, next, filterByTable[tableName2])
            }, filterByTable[tableName])
        }
    }
}