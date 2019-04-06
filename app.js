const _ = require('lodash');
const fs = require('fs');
const get = require('./read')
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size * 400;

const letter = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"];

if (!fs.existsSync('./test')) {
    fs.mkdirSync('./test/');
}
let columnNumberDict = {};
let cardinalityDict = {};
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
    rl.on('line', (line) => {
        lineNumber++;
        if (columnNumber === 0) {
            columnNumber = 1;
            for (let i = 0; i < line.length; i++) {
                if (line.charAt(i) === ',') {
                    columnNumber++
                }
            }
            columnNumberDict[path] = columnNumber;
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
        cardinalityDict[table_name] = lineNumber;
        //console.log(new Date().getTime() - start)
        wlArray.length && wlArray.forEach((wl, index) => {
            wl.write(bufArray[index].slice(0, bufferIndexArray[index]), () => {
                wl.close();
                console.log(new Date().getTime() - start)
            })
        })
    });
}