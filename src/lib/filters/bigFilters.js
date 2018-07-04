/**
 * Pond filters.
 */

const _ = require('lodash')
const math = require('../math')
const {filter} = require('pondjs')

function isValid (v) {
  return !(_.isUndefined(v) || v.isNaN() || _.isNull(v))
}

const ignoreMissing = (values) => values.filter(isValid)

const propagateMissing = (values) => ignoreMissing(values).length === values.length ? values : null

const zeroMissing = (values) => values.map(v => (isValid(v) ? v : math.bignumber(0)))

module.exports = {
  ignoreMissing,
  keepMissing: filter.keepMissing,
  noneIfEmpty: filter.noneIfEmpty,
  propagateMissing,
  zeroMissing
}
