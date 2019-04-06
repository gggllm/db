const _ = require('lodash');
const fs = require('fs');
const {get, write} = require('./util');
const parse = require('./parser');
const optimize = require('./optimizer');
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size * 400;

const letter = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"];

if (!fs.existsSync('./test')) {
    fs.mkdirSync('./test/');
}
let metaDict = {};
let inMemoryDataBase = {};

for (let i = 0; i < 17; i++) {
    build(letter[i])
}


function build(table_name) {
    let path = `./pa3_data/data/l/${table_name}.csv`;

    let write;
    let bufArray = [];
    let wlArray = [];
    let bufferIndexArray = [];
    if (fs.statSync(path).size < 501732673) {
        let ds = [];
        inMemoryDataBase[table_name] = ds;
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
            if (bufferIndex + 4 >= buffer_size) {
                wl.write(buf);
                bufferIndex = 0
            }
            bufferIndexArray[index] = bufferIndex
        }
    }
    let start = new Date().getTime();

    let rl = readFileByLine(path);

// use buffer to write one block at a time


    let columnNumber = 0;
    let lineNumber = 0;
    let metaData = {};
    metaDict[path] = metaData;
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
                let wl = fs.createWriteStream(`./test/${table_name}${i}.bin`);
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
            wl.write(bufArray[index].slice(0, bufferIndexArray[index]), () => {
                wl.close();
                console.log(new Date().getTime() - start)
            })
        })
    });
}

function query(input) {
    let [select, from, where, filter] = parse(input);
    // get the join sequence and tables that is needed for extraction
    let {joins, tables} = optimize(select, from, where, filter, metaDict);
    let acc = {};
    let joinNum = 0
    join(joins[joinNum++])

    function join({tableName, tableName2, column, column2}) {
        // make sure table 1 is smaller then table 2
        if (metaDict[tableName] > metaDict[tableName2]) {
            let i = tableName;
            tableName = tableName2;
            tableName2 = i;
            i = column;
            column = column2;
            column2 = i
        }
        let db1 = new Map();
        let db2 = new Map();
        get(tableName, column, (value, index) => {
            let list = db1.get(value) || [];
            list.push(index);
            db1.set(value, list)
        }, inMemoryDataBase, () => {
            get(tableName2, column2, (value, index) => {
                // if found the target, we just store the relationship we need
                if (db1.get(value)) {
                    let list = db2.get(value) || [];
                    list.push(index);
                    db2.set(value, list)
                }// if no same drop
            }, inMemoryDataBase, () => {
                // build a filtered db1
                let ndb1 = new Map();
                for ({key, value} of db1) {
                    if (db2.has(key)) {
                        ndb1.set(key, value)
                    }
                }

                join(joins[joinNum++])
            })
        })
    }
}