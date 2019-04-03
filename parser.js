const _ = require('lodash');

function parse(input) {
    [select, from, where, final] = input.split('\r');
    parse_select(select);
    parse_from(from);
    parse_where(where);
    parse_final(final)
}

function parse_select() {

}

function parse_from() {

}

function parse_where(where) {
    return _.trim(where).split(/,\s*/)
}

function parse_final(final) {

}