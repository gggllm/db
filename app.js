const _ = require('lodash')
const fs = require('fs')

const readFileByLine = require('./readFileByLine')
const path = './pa3_data/data/s/A.csv'

rl = readFileByLine(path)

rl.on('line', (line) => {
    console.log(`Line from file: ${line}`);
});