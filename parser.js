const _ = require('lodash');

function parse(input) {
    [select, from, where, final] = input.split('\r');
    parse_select(select);
    parse_from(from);
    parse_where(where);
    parse_final(final)
}

function parse_select(select) {
    let regex = /SUM\(\w\.\w\d\)/;
    let match;
    let result = [];
    while (match = regex.exec(select)) {
        result.push(match[0])
        select = select.substring(match.index+result.length)
    }
    return result
}

console.log(parse_select('SELECT SUM(A.c6), SUM(A.c2)'));

function parse_from(from) {
    let regex = /SUM\(\w\.\w\d\)/;
    let match;
    let result = [];
    while (match = regex.exec(from)) {
        result.push(match[0])
    }
    return result
}

function parse_where(where) {
    return _.trim(where).split(/,\s*/)
}

function parse_final(final) {

}