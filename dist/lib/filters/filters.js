'use strict';

/**
 * Pond filters.
 */

const { filter } = require('pondjs');

module.exports = {
  ignoreMissing: filter.ignoreMissing,
  keepMissing: filter.keepMissing,
  noneIfEmpty: filter.noneIfEmpty,
  propagateMissing: filter.propagateMissing,
  zeroMissing: filter.zeroMissing
};