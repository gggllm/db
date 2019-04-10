const _ = require('lodash');

function estimateCardinality() {

}

function optimize(select, from, where, filter, metaData) {
    let tables = {};
    let best = {};
    let cache = {};

    function calculateSimpleJoinSize({tableName, tableName2, columns, columns2}) {

        let meta1 = metaData[tableName];
        let meta2 = metaData[tableName2];
        let size1 = meta1.size;
        let size2 = meta2.size;
        let size = size1 * size2;
        columns.forEach((column, index) => {
            let column2 = columns2[index];
            let unique1 = meta1.unique[column];
            let unique2 = meta2.unique[column2];
            size = size * Math.min(unique1, unique2) / (unique1 * unique2);
        });
        let result = {size};
        result[tableName] = meta1;
        result[tableName2] = meta2;
        let res = {size};
        res[tableName] = meta1;
        res[tableName2] = meta2;
        cache[[tableName, tableName2].sort().join('')] = res;
        return size
    }

    function getCache(rel) {
        let last = cache[rel]
        if (last) {
            return last
        }
        return 999999999
    }

    function calculateSize(rel, {tableName, tableName2, columns, columns2}) {
        // transform to array
        if (!columns.push) {
            columns = [columns];
            columns2 = [columns2]
        }
        if (rel.length === 1) {
            return calculateSimpleJoinSize({tableName, tableName2, columns, columns2})
        }
        if (rel.indexOf(tableName) < 0) {
            let i = tableName;
            tableName = tableName2;
            tableName2 = i;
            i = columns;
            columns = columns2;
            columns2 = i
        }

        let resRel = [...rel, tableName2].sort().join('');
        if (cache[resRel]) {
            let cr = cache[resRel];
            return cr.size
        }
        let last = getCache(rel.join(''));
        // cannot reach
        if (!last.size) {
            return last
        }
        let meta = metaData[tableName2];
        let size = last.size * meta.size;
        columns2.forEach((column2, index) => {
            let column = columns[index];
            let unique = meta.unique[column];
            let unique2 = last[tableName].unique[column];
            size = size * Math.min(unique2, unique) / (unique2 * unique)
        });
        let res = {...last, size};
        res[tableName2] = meta;
        cache[resRel] = res;
        return size
    }

    // find joins using rels and r
    function getJoin(rels, r) {
        let res = []
        for (let i = 0; i < rels.length; i++) {
            let rel = rels[i]
            if (joinMap[rel + ',' + r]) {
                res.push(joinMap[rel + ',' + r])
            }
            if (joinMap[r + ',' + rel]) {
                res.push(joinMap[r + ',' + rel])
            }
        }
        return res
    }

//rels is an array
    function computeBest(rels) {
        let symbol = rels.join('');
        if (best[symbol]) {
            return best[symbol]
        }
        // base case
        if (rels.length === 1) {
            return [metaData[rels[0]].size, rels]
        }
        let curr = 9999999999;
        let p = []
        for (let r of rels) {
            let relNew = rels.filter((val) => val !== r);
            let [internalOrder, path] = computeBest(relNew);
            let totalCost = internalOrder + cost(relNew, r);
            if (totalCost < curr) {
                p = [...path];
                curr = totalCost
                p.push(r)
            }
        }
        best[symbol] = [curr, p];
        return best[symbol]
    }

    function cost(rel, r) {
        let join = getJoin(rel, r);
        if (join.length === 0) {
            return 9999999
        }
        // still problematic
        return calculateSize(rel, join[0])
    }

    function bestToJoins(best) {
        let rel = [best[0]];
        let joins = [];
        while (rel.length < best.length) {
            joins = _.concat(joins, getJoin(rel, best[rel.length]));
            rel.push(best[rel.length])
        }
        return joins
    }


    select.forEach((([tableName, column]) => {
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column)
    }));
    let joinMap = {}
    where.forEach((([tableName, column, tableName2, column2]) => {
        if (tableName > tableName2) {
            let i = tableName2
            tableName2 = tableName
            tableName = i
            i = column
            column = column2
            column2 = i
        }
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column);
        let table2 = tables[tableName2] || new Set();
        tables[tableName2] = table2;
        table2.add(column2);
        let join = joinMap[tableName + ',' + tableName2] || {
            tableName, tableName2, columns: [], columns2: []
        }
        joinMap[tableName + ',' + tableName2] = join
        join.columns.push(column)
        join.columns2.push(column2)
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

    function calculateTableIndex() {
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
        return tableIndex;
    }

    let joins = bestToJoins(computeBest(from)[1]);
// store the column's position in the resulting row
    let tableIndex = calculateTableIndex();

    // _.sortBy(joins, ({tableName, tableName2, column, column2}) => {
    //     return Math.min(tableMetaData[tableName].size, tableMetaData[tableName2].size)
    // });
    return {joins, tables, tableIndex, filterByTable};

}


module.exports = optimize;