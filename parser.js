const _ = require('lodash');

function parse(input) {
    let select, from, where, filter;
    [select, from, where, filter] = input.split(/\n\s*/);
    select = parse_select(select).map((value) => {
        return [value.substr(4, 1), parseInt(value.substring(7, value.length - 1))]
    });
    from = parse_from(from);
    where = parse_where(where).map((value) => {
        let firstTable = value.substr(0, 1);
        value = value.substr(3);
        let firstColumn;
        let i;
        for (i = 0; i < value.length; i++) {
            if (value.charAt(i) === ' ') {
                firstColumn = parseInt(value.substring(0, i));
                break
            }
        }
        let secondTable = value.substr(i + 3, 1);

        return [firstTable, firstColumn, secondTable, parseInt(value.substring(i + 7, value.length - 1))]
    });
    filter = parse_filter(filter).map((value) => {
        let firstTable = value.substr(0, 1);
        value = value.substr(3);
        let firstColumn;
        let i;
        for (i = 0; i < value.length; i++) {
            if (value.charAt(i) === ' ') {
                firstColumn = parseInt(value.substring(0, i));
                break
            }
        }
        let operator = value.substr(i + 1, 1);

        return [firstTable, firstColumn, operator, parseInt(value.substring(i + 3, value.length))]
    });

    //console.log(select, from, where, filter)
    return [select, from, where, filter]
}

// parse(`SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)
//        FROM A,B,C,D
//        WHERE A.c1 = B.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
//         AND D.c3 > -9496;`)

function parse_select(select) {
    let regex = /SUM\(\w\.c\d+\)/;
    let match;
    let result = [];
    while (match = regex.exec(select)) {
        result.push(match[0]);
        select = select.substring(match.index + match[0].length)
    }
    return result
}

//console.log(parse_select('SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)'))


function parse_from(from) {
    let regex = /\w/;
    let match;
    let result = [];
    from = from.substr(5);
    while (match = regex.exec(from)) {
        result.push(match[0]);
        from = from.substring(match.index + match[0].length)
    }
    return result
}

//console.log(parse_from('FROM A, C, D'));

function parse_where(where) {
    let regex = /\w\.c\d+\s=\s\w\.c\d+/;
    let match;
    let result = [];
    while (match = regex.exec(where)) {
        result.push(match[0]);
        where = where.substring(match.index + match[0].length)
    }
    return result
}

//console.log(parse_where('WHERE A.c1 = B.c0 AND A.c3 = D.c0 AND C.c2 = D.c2'))

function parse_filter(final) {
    let regex = /\w\.c\d+\s[=><]\s-*\d+/;
    let match;
    let result = [];
    while (match = regex.exec(final)) {
        result.push(match[0]);
        final = final.substring(match.index + match[0].length)
    }
    return result
}

//console.log(parse_filter('AND A.c4 = -9868;'))

module.exports = parse