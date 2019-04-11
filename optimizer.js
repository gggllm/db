const _ = require('lodash');

function estimateCardinality() {

}

function optimize(select, from, where, filter, metaData) {
    metaData = _.cloneDeep(metaData);
    let tables = {};
    let best = {};
    let cache = {};

    function calculateSimpleJoinSize(joins) {
        let tableName = joins[0].tableName;
        let tableName2 = joins[0].tableName2;
        let meta1 = metaData[tableName];
        let meta2 = metaData[tableName2];
        let size1 = meta1.size;
        let size2 = meta2.size;
        let size = size1 * size2;
        joins.forEach(({column, column2}) => {
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
        let last = cache[rel];
        if (last) {
            return last
        }
        return 9999999999
    }

    //Todo
    function calculateSize(rel, addedTableName, joins) {
        if (!joins.push) {
            joins = [joins]
        }
        if (rel.length === 1) {
            return calculateSimpleJoinSize(joins)
        }
        let resRel = [...rel, addedTableName].sort().join('');
        if (cache[resRel]) {
            let cr = cache[resRel];
            return cr.size
        }
        let last = getCache(rel.join(''));
        // cannot reach
        if (!last.size) {
            return last
        }
        let meta = metaData[addedTableName];
        let size = last.size * meta.size;
        // hack
        let res = {...last};
        res[addedTableName] = meta;
        joins.forEach(({tableName, tableName2, column, column2}) => {
            let unique = res[tableName].unique[column];
            let unique2 = res[tableName2].unique[column2];
            size = size * Math.min(unique2, unique) / (unique2 * unique)
        });
        res.size = size;
        cache[resRel] = res;
        return size
    }

    // find joins using rels and r
    // we should make sure the joined table in the table2
    function getJoin(rels, r) {
        let res = [];
        for (let i = 0; i < rels.length; i++) {
            let rel = rels[i];
            let join = joinMap[rel + ',' + r];
            if (join) {
                join.columns.forEach((column, index) => {
                    let column2 = join.column2[index];
                    let tableName = join.tableName;
                    let tableName2 = join.tableName2;
                    if (tableName === r) {
                        let i = tableName;
                        tableName = tableName2;
                        tableName2 = i;
                        i = column;
                        column = column2;
                        column2 = i
                    }
                    res.push({tableName, tableName2, column, column2})
                })
            }
            join = joinMap[r + ',' + rel];
            if (join) {
                join.columns.forEach((column, index) => {
                    let column2 = join.column2[index];
                    let tableName = join.tableName;
                    let tableName2 = join.tableName2;
                    if (tableName === r) {
                        let i = tableName;
                        tableName = tableName2;
                        tableName2 = i;
                        i = column;
                        column = column2;
                        column2 = i
                    }
                    res.push({tableName, tableName2, column, column2})
                })
            }
        }
        return res.length === 1 ? res[0] : res
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
        let curr;
        let p = [];
        for (let r of rels) {
            let relNew = rels.filter((val) => val !== r);
            let [internalOrder, path] = computeBest(relNew);
            if (!internalOrder) {
                continue
            }
            let c = cost(relNew, r);
            if (c === null) {
                continue
            }
            let totalCost = internalOrder + c;
            if (totalCost < curr || !curr) {
                p = [...path];
                curr = totalCost;
                p.push(r)
            }
        }
        best[symbol] = [curr, p];
        return best[symbol]
    }

    function cost(rel, r) {
        let join = getJoin(rel, r);
        if (join.length === 0) {
            return null
        }
        // still problematic
        return calculateSize(rel, r, join)
    }

    function bestToJoins(best) {
        let rel = [best[0]];
        let joins = [];
        while (rel.length < best.length) {
            joins.push([[...rel], best[rel.length], getJoin(rel, best[rel.length])]);
            rel.push(best[rel.length])
        }
        return joins
    }

    let useSituation = {};


    select.forEach((([tableName, column]) => {
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column);
        let use = useSituation[tableName + column] || 0;
        use++;
        useSituation[tableName + column] = use
    }));
    let joinMap = {};
    where.forEach((([tableName, column, tableName2, column2]) => {
        if (tableName > tableName2) {
            let i = tableName2;
            tableName2 = tableName;
            tableName = i;
            i = column;
            column = column2;
            column2 = i
        }
        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column);
        let table2 = tables[tableName2] || new Set();
        tables[tableName2] = table2;
        table2.add(column2);
        let join = joinMap[tableName + ',' + tableName2] || {
            tableName, tableName2, columns: [], column2: []
        };
        joinMap[tableName + ',' + tableName2] = join;
        join.columns.push(column);
        join.column2.push(column2);

        let use = useSituation[tableName + column] || 0;
        use++;
        useSituation[tableName + column] = use;

        use = useSituation[tableName2 + column2] || 0;
        use++;
        useSituation[tableName2 + column2] = use
    }));
    let filterByTable = {};
    let tableP = {};
    filter.forEach((([tableName, column, operator, target]) => {

        let use = useSituation[tableName + column] || 0;
        use++;
        useSituation[tableName + column] = use;

        let table = tables[tableName] || new Set();
        tables[tableName] = table;
        table.add(column);
        let filters = filterByTable[tableName] || [];
        filterByTable[tableName] = filters;
        let p = tableP[tableName] || [];
        tableP[tableName] = p;
        let meta = metaData[tableName];
        let gap = meta.max[column] - meta.min[column];
        switch (operator) {
            case '<':
                filters.push([column, (value) => {
                    return value < target
                }]);
                p.push((target - meta.min[column]) / (gap));
                break;
            case '=':
                filters.push([column, (value) => {
                    return value === target
                }]);
                p.push(1 / meta.unique[column]);
                break;
            case '>':
                filters.push([column, (value) => {
                    return value > target
                }]);
                p.push((meta.max[column] - target) / (gap))
        }
    }));

    for (let tableName in tableP) {
        metaData[tableName].size = tableP[tableName].reduce((acc, cur) => acc * cur) * metaData[tableName].size
    }


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


    // get a table column that will removed column that will only be used in filter

    let accIndex = calculateAccIndex(_.flatMap(joins, 2), tables)

    function calculateAccIndex(joins, tables,useSituation) {
        function addIndex(tableName) {
            let tIndex = {};
            accIndex[tableName] = tIndex;
            let curIndex = tableIndex[tableName];
            for (let col in curIndex) {
                tIndex[col] = accLength + curIndex[col]
            }
            accLength += tables[tableName].length
        }

        let accIndex = {};
        let accLength = 0;
        let smallTable = _.cloneDeep(tables)
        for (let table in filterByTable) {
            let filters=filterByTable[table]
            for(let i=0;i<filters.length;i++){
                let [column,]=filters[i]
                let count=tables[tables][column]

            }
            let removedColumn = _(filterByTable[table]).filter(([column, filter]) => useSituation[column] === 1).groupBy(0).value;
            smallTable[table] = tables[table].filter((col) => !removedColumn[col])
        }
        joins.forEach(({tableName, tableName2, column, column2}) => {
            if (!accIndex[tableName]) {
                addIndex(tableName)
            }
            if (!accIndex[tableName2]) {
                addIndex(tableName2)
            }
        });
        return accIndex
    }

    return {joins, tables, tableIndex, filterByTable, useSituation, accIndex};

}


module.exports = optimize;