const _ = require('lodash');
const fs = require('fs');
const Stream = require('stream');
const readFileByLine = require('./readFileByLine');
// multiply by 4 to make sure it can fit by integer without padding
const block_size = (fs.statSync('./app.js').blksize || 4096);
const buffer_size = block_size * 4;

const letter = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"];

for (let i = 0; i < 17; i++) {
    build(letter[i])
}

function build(num) {
    let path = `./pa3_data/data/L/${num}.csv`;
    let start = new Date().getTime();

    let rl = readFileByLine(path);
    let wl = fs.createWriteStream(`./test/test${num}.bin`);

// use buffer to write one block at a time
    let buf = Buffer.allocUnsafe(buffer_size);

    let bufferIndex = 0;

    function write(wl, item) {
        buf.writeInt32LE(item, bufferIndex);
        bufferIndex += 4;
        if (bufferIndex + 4 >= buffer_size) {
            wl.write(buf);
            bufferIndex = 0
        }
    }

    rl.on('line', (line) => {
        let length = line.length;
        let acc = 0;
        let flag = 1;


        for (let i = 0; i < length; i++) {
            let ch = line.charAt(i);
            if (ch === ',') {
                write(wl, acc * flag);
                acc = 0;
                flag = 1
            } else if (ch === '-') {
                flag = -1
            } else {
                acc = acc * 10 + (ch - 0)
            }
        }
        write(wl, acc * flag)
    });


    rl.on('close', () => {
        //console.log(new Date().getTime() - start)
        wl.write(buf.slice(0, bufferIndex), () => {
            wl.close();
            console.log(new Date().getTime() - start)
        })
    });
}