"use strict";

/**
 * Pond aggregators.
 */
const pond = require('pondjs');

module.exports = {
  avg: pond.avg,
  count: pond.count,
  difference: pond.difference,
  first: pond.first,
  keep: pond.keep,
  last: pond.last,
  max: pond.max,
  median: pond.median,
  min: pond.min,
  percentile: pond.percentile,
  stdev: pond.stdev,
  sum: pond.sum
};