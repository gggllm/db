function readFromFile(table, col) {
    return fs.createReadStream(`./test/${table}${col}.bin`, {encoding: 'buffer'})
}

function get(table, col, cb) {
    if (inMemoryDataBase[table]) {
        return _(inMemoryDataBase[table]).map(col).forEach(cb)
    } else {
        let rl = readFromFile(table, col);
        let count = 0;
        rl.on('data', () => {
            let buf = this.readInt32LE();
            while (buf) {
                cb(buf, count++);
                buf = this.readInt32LE();
            }
        })
    }
}

module.exports = get