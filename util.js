function readFromFile(table, col) {
    return fs.createReadStream(`./test/${table}${col}.bin`, {encoding: 'buffer'})
}

function get(table, col, cb, inMemoryDataBase, cb2) {
    if (inMemoryDataBase[table]) {
        _(inMemoryDataBase[table]).map(col).forEach(cb)
        cb2 && cb2()
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
        rl.on('close', () => {
            cb2 && cb2()
        })
    }
}

function write(table, tableName, inMemoryDatabase) {
    inMemoryDatabase[tableName] = table
}


module.exports = {get, write}