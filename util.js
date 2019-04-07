const _ = require("lodash");

function readFromFile(table, col) {
    return fs.createReadStream(`./test/${table}${col}.bin`, {encoding: 'buffer'})
}

// cb is for streaming purpose
// cb2 is for running sequentially

function get(table, colums, cb, inMemoryDataBase, cb2, filters = []) {
    if (inMemoryDataBase[table]) {
        _(inMemoryDataBase[table]).map((value) => {
            return colums.map((column => {
                return value[column]
            }))
        }).filter((row) => {
            let column, filter;
            for ([column, filter] in filters) {
                if (!filter(row[column])) {
                    return false
                }
            }
            return true
        }).forEach(cb);
        cb2 && cb2()
    } else {
        let db = [];
        let sizeArray = [];
        let cursor = 0;
        let finished = 0;
        let length = colums.length;
        filters = _.groupBy(filters, 0);
        colums.forEach((col, index) => {
            let rl = readFromFile(table, col);
            let count = 0;
            let colFilters = filters[col].map(1);
            rl.on('data', () => {
                let buf = this.readInt32LE();
                while (buf) {
                    let dropFlag = false;
                    for (let filter of colFilters) {
                        if (!filter(buf)) {
                            buf = this.readInt32LE();
                            dropFlag = true;
                            break
                        }
                    }
                    if (dropFlag) {
                        db[count] = null;// delete this row
                        count++;
                        break
                    }
                    let row = db[count] || [];
                    db[count] = row;
                    let size = sizeArray[count] || 0;
                    sizeArray[count] = ++size;
                    row[index] = buf;
                    count++;
                    if (size === length) {
                        cb(row);
                        db[count] = null //delete the finished row
                    }
                    buf = this.readInt32LE();
                }
            });
            rl.on('close', () => {
                if (++finished === length) {
                    cb2 && cb2()
                }
            })
        })
    }
}

function write(table, tableName, inMemoryDatabase) {
    inMemoryDatabase[tableName] = table
}


module.exports = {get, write};