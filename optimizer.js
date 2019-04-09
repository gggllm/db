const _ = require('lodash');

function estimateCardinality() {

}

function optimize(select, from, where, filter, metaData) {
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
        filterByTable[tableName] = filters;
        switch (operator) {
            case '<':
                filters.push([column, (value) => {
                    return value < target
                }]);
                break;
            case '=':
                filters.push([column, (value) => {
                    return value === target
                }]);
                break;
            case '>':
                filters.push([column, (value) => {
                    return value > target
                }])
        }
    }));
    // store the column's position in the resulting row
    let tableIndex = {};
    for (let key in  tables) {
        let value = tables[key];
        let curIndex = {};
        tableIndex[key] = curIndex;
        let array = [...value];
        // change set to array
        tables[key] = array;
        array.sort((a, b) => a > b);
        array.forEach((value, index) => {
            curIndex[value] = index
        })
    }
    // _.sortBy(joins, ({tableName, tableName2, column, column2}) => {
    //     return Math.min(tableMetaData[tableName].size, tableMetaData[tableName2].size)
    // });
    return {joins, tables, tableIndex, filterByTable};


    let best = {};
    let cache = {};

    function calculateSimpleJoinSize({tableName, tableName2, column, column2}) {

        let meta1 = metaData[tableName];
        let meta2 = metaData[tableName2];
        let unique1 = meta1.unique[column];
        let size1 = meta1.size;
        let size2 = meta2.size;
        let unique2 = meta2.unique[column2];
        let size = size1 * size2 / Math.min(unique1, unique2) / (unique1 * unique2);
        let result = {size};
        result[tableName] = meta1;
        result[tableName2] = meta2;
        let res = {size}
        res[tableName] = meta1
        res[tableName2] = meta2
        cache[[tableName, tableName2].sort().join('')] = res
        return size
    }

    function calculateSize(rel, {tableName, tableName2, column, column2}) {
        if (rel.indexOf(tableName) < 0) {
            let i = tableName;
            tableName = tableName2;
            tableName2 = i;
            i = column;
            column = column2;
            column2 = i
        }
        // means none of them have joined
        if (rel.indexOf(tableName) < 0) {
            return calculateSimpleJoinSize({tableName, tableName2, column, column2})
        }
        let resRel = [...rel, tableName].sort().join('');
        if (cache[resRel]) {
            return cache[resRel].size
        }
        let last = cache[rel];
        let size;
        if (!last[tableName]) {
            size = 999999999
        } else {
            let meta = metaData[tableName];
            let unique = meta.unique[column];
            size = last.size * meta.size * Math.min(last[tableName].unique[column], unique) / (last[tableName].unique[column] * unique)
        }

    }


    function queryOptimize(joins) {

    }

//rels is an array
    function computeBest(rels) {
        let symbol = rels.join('');
        if (best[symbol]) {
            return best[symbol]
        }
        let curr = 9999999999;
        for (let r of rels) {
            let internalOrder = computeBest(_.remove(rels, r));
            let totalCost = internalOrder + cost(rel, r);
            curr = Math.min(totalCost, curr)
        }
        best[symbol] = curr
    }

    function cost(rel, r) {

    }

}


module.exports = optimize;