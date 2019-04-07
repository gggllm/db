const _ = require('lodash');

function estimateCardinality() {

}

function optimize(select, from, where, filter, tableMetaData) {
    let tables = {};
    let joins = [];
    select.forEach((([tableName, column]) => {
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column)
    }));
    where.forEach((([tableName, column, tableName2, column2]) => {
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column);
        let table2 = tables[tableName2] || new Set();
        tables[tableName2] = table2;
        table2.add(column2);
        joins.push({
            tableName, tableName2, column, column2
        })
    }));
    let filterByTable = {};
    filter.forEach((([tableName, column, operator, target]) => {
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column);
        let filters = filterByTable[tableName] || [];
        filterByTable[tableName] = filters
        switch (operator) {
            case '<':
                filters.push([column, (value) => {
                    return value < target
                }])
                break
            case '=':
                filters.push([column, (value) => {
                    return value === target
                }])
                break
            case '>':
                filters.push([column, (value) => {
                    return value > target
                }])
        }
    }));
    // store the column's position in the resulting row
    let tableIndex = {};
    for ({key, value}of tables) {
        let index = {};
        tableIndex[key] = index;
        let array = Array(value);
        array.sort((a, b) => a > b);
        array.forEach((value, index) => {
            index[value] = index
        })
    }
    // _.sortBy(joins, ({tableName, tableName2, column, column2}) => {
    //     return Math.min(tableMetaData[tableName].size, tableMetaData[tableName2].size)
    // });
    return {joins, tables, tableIndex}
}

module.exports = optimize;