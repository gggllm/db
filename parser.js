const _ = require('lodash');

function parse(input) {
    [select, from, where, final] = input.split('\r|\n|\r\n');
    parse_select(select);
    parse_from(from);
    parse_where(where);
    parse_final(final)
}

function parse_select(select) {
    let regex = /SUM\(\w\.\c\d\)/;
    let match;
    let result = [];
    while (match = regex.exec(select)) {
        result.push(match[0])
        select = select.substring(match.index + result.length)
    }
    return result
}

function parse_from(from) {
    return _.trim(where).split(/,\s*/)
}

function parse_where(where) {
    let regex = /\w\.\c\d\s=\s\w\.\c\d/;
    let match;
    let result = [];
    while (match = regex.exec(from)) {
        result.push(match[0])
    }
    return result
}

function parse_final(final) {
    let regex = /SUM\(\w\.\w\d\)/;
    let match;
    let result = [];
    while (match = regex.exec(from)) {
        result.push(match[0])
    }
    return result
}