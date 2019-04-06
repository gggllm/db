const _ = require('lodash')

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
    filter.forEach((([tableName, column, ...rest]) => {
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column)
    }));
    _.sortBy(joins, ({tableName, tableName2, column, column2}) => {
        return Math.min(tableMetaData[tableName].size, tableMetaData[tableName2].size)
    });
    return {joins, tables}
}

module.exports = optimize;