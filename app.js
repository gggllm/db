const _ = require('lodash');
const fs = require('fs');
const Stream = require('stream');
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size * 4;

const letter = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"];

if (!fs.existsSync('./test')) {
    fs.mkdirSync('./test/');
}
let columnNumberDict = {};
let inMemoryDataBase = {};

for (let i = 0; i < 16; i++) {
    build(letter[i])
}


function build(num) {
    let path = `./pa3_data/data/m/${num}.csv`;

    let write;

    let wl;
    if (fs.statSync(path).size < 5000000) {
        let ds = [];
        inMemoryDataBase[num] = ds;
        let index = 0;
        let cur = [];
        write = (item) => {
            cur.push(item);
            index++;
            index = index % columnNumber;
            if (index === 0) {
                cur = [];
                ds.push(cur)
            }
        }
    } else {
        wl = fs.createWriteStream(`./test/test${num}.bin`);
        write = function (item) {
            buf.writeInt32LE(item, bufferIndex);
            bufferIndex += 4;
            if (bufferIndex + 4 >= buffer_size) {
                wl.write(buf);
                bufferIndex = 0
            }
        }
    }
    let start = new Date().getTime();

    let rl = readFileByLine(path);

// use buffer to write one block at a time
    let buf = Buffer.allocUnsafe(buffer_size);

    let bufferIndex = 0;


    let columnNumber = 0;
    rl.on('line', (line) => {
        if (columnNumber === 0) {
            columnNumber = 1;
            for (let i = 0; i < line.length; i++) {
                if (line.charAt(i === ',')) {
                    columnNumber++
                }
            }
            columnNumberDict[path] = columnNumber
        }
        let length = line.length;
        let acc = 0;
        let flag = 1;


        for (let i = 0; i < length; i++) {
            let ch = line.charAt(i);
            if (ch === ',') {
                write(acc * flag);
                acc = 0;
                flag = 1
            } else if (ch === '-') {
                flag = -1
            } else {
                acc = acc * 10 + (ch - 0)
            }
        }
        write(acc * flag)
    });


    rl.on('close', () => {
        //console.log(new Date().getTime() - start)
        wl && wl.write(buf.slice(0, bufferIndex), () => {
            wl.close();
            console.log(new Date().getTime() - start)
        })
    });
}