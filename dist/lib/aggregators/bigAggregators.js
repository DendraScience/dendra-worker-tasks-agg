'use strict';

/**
 * Pond aggregators.
 */

const math = require('../math');
const pond = require('pondjs');

function avg(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    return math.mean(cleanValues);
  };
}

function difference(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    return math.subtract(math.max(clean), math.min(cleanValues));
  };
}

function keep(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    const result = pond.first()(cleanValues);
    cleanValues.forEach(v => {
      if (math.unequal(v, result)) {
        return null;
      }
    });
    return result;
  };
}

function max(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    const result = math.max(cleanValues);
    if (result.isFinite()) {
      return result;
    }
  };
}

function median(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    return math.median(cleanValues);
  };
}

function min(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    const result = math.min(cleanValues);
    if (result.isFinite()) {
      return result;
    }
  };
}

function std(normalization = 'unbiased', clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    return math.std(cleanValues, normalization);
  };
}

function stdev(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    return math.std(cleanValues, 'uncorrected');
  };
}

function sum(clean = pond.filter.ignoreMissing) {
  return values => {
    const cleanValues = clean(values);
    if (!cleanValues) {
      return null;
    }
    return math.sum(cleanValues);
  };
}

module.exports = {
  avg,
  count: pond.count,
  difference,
  first: pond.first,
  keep,
  last: pond.last,
  max,
  median,
  min,
  std,
  stdev,
  sum
};