/**
 * Method to fetch datapoints and perform rollup steps.
 */

const jsonata = require('jsonata')
const math = require('../../../lib/math')
const moment = require('moment')
const pick = require('lodash/pick')
const { aggregators, bigAggregators } = require('../../../lib/aggregators')
const { filters, bigFilters } = require('../../../lib/filters')
const { duration, timeSeries, window } = require('pondjs')
const { MomentEditor } = require('@dendra-science/utils-moment')

const QUERY_DEFAULTS = {
  time_local: true
}

const SPEC_DEFAULTS = {
  big_math: false,
  shift: '',
  time_cursor: '1_d',
  transform: '{"columns": ["time", "v"], "points": $.[$time(t, o, "s"), $value(v)]}'
}

/**
 * Fetch datapoints, transform and shift along the way.
 */
async function findAndTransform (spec, { datapointService, logger }) {
  /*
    Detect editor format (relative to now) or assume absolute time.
   */

  const begEditor = new MomentEditor(spec.time_gte)
  const endEditor = new MomentEditor(spec.time_lt)
  const now = moment.utc()
  let begTime = (begEditor.fns.length > 0) ? begEditor.edit(now) : moment.utc(spec.time_gte)
  const endTime = (endEditor.fns.length > 0) ? endEditor.edit(now) : moment.utc(spec.time_lt)

  const cursorArgs = spec.time_cursor.split('_')

  /*
    Prepare editor (for shifting) and transformation expression.
   */

  const shiftEditor = new MomentEditor(spec.shift)
  const expr = jsonata(spec.transform)

  expr.registerFunction('time', (time, offset = 0, unit = 'ms') => {
    return shiftEditor.edit(moment.utc(time).add(offset, unit)).valueOf()
  }, '<(sn)n?s?:n>')

  expr.registerFunction('value', (value) => {
    return spec.big_math && (value !== null) ? math.bignumber(value) : value
  }, '<(nl):(no)>')

  let columns = []
  let points = []

  /*
    Fetch, transform and accumulate datapoints.
   */

  while (begTime < endTime) {
    const curTime = moment.min(begTime.clone().add(...cursorArgs), endTime)
    const query = Object.assign({}, QUERY_DEFAULTS, spec.query, {
      time: {
        $gte: begTime.toISOString(),
        $lt: curTime.toISOString()
      },
      $limit: 2000,
      $sort: {
        time: 1 // ASC
      }
    })

    logger.info('Find and transform', { query })

    // TODO: Send auth_info.jwt in header, see https://docs.feathersjs.com/api/client/rest.html#paramsheaders
    const res = await datapointService.find({
      query
    })

    if (res.data && (res.data.length > 0)) {
      const data = await new Promise((resolve, reject) => {
        expr.evaluate(res.data, {}, (err, res) => err ? reject(err) : resolve(res))
      })

      if (Array.isArray(data.columns) && Array.isArray(data.points)) {
        if (columns.length === 0) columns = data.columns
        points = points.concat(data.points)
      }
    }

    await new Promise(resolve => setImmediate(resolve))

    begTime = curTime.clone()
  }

  return {
    columns,
    points
  }
}

function optionsForRollup (rollup, { big_math: big }) {
  const aggregation = rollup.aggregations.reduce((agg, { alias, args, field, filter, func }) => {
    if (typeof func !== 'string') throw new Error('Invalid aggregation function')
    if (typeof field !== 'string') throw new Error('Invalid aggregation field')

    if (typeof alias !== 'string') alias = `${field}_${func}`

    const aggFn = big ? bigAggregators[func] : aggregators[func]

    if (!aggFn) throw new Error('Unknown aggregation function')

    if (typeof args === 'undefined') args = []
    if (!Array.isArray(args)) throw new Error('Invalid aggregation arguments')

    if (typeof filter !== 'undefined') {
      if (typeof filter !== 'string') throw new Error('Invalid aggregation filter')

      const filterFn = big ? bigFilters[filter] : filters[filter]
      if (!filterFn) throw new Error('Unknown aggregation filter')

      // The filter is always the last arg
      if (filterFn) args.push(filterFn)
    }

    agg[alias] = [field, aggFn(...args)]

    return agg
  }, {})

  return {
    // TODO: Support moment.duration (a Pond bug prevents this for now)
    window: window(duration(rollup.window.split('_').join(''))),
    aggregation
  }
}

async function rollupDatapoints (req, ctx) {
  // TODO: Add more logging
  // const { logger } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)

  /*
    Fetch datapoints, transform and shift along the way.
   */

  let series = timeSeries(await findAndTransform(spec, ctx))

  /*
    Process rollups.
   */

  if (spec && Array.isArray(spec.rollups)) {
    for (const rollup of spec.rollups) {
      series = series.fixedWindowRollup(optionsForRollup(rollup, spec))

      await new Promise(resolve => setImmediate(resolve))
    }
  }

  /*
    Serialize and return.
   */

  return series.eventList().map(e => {
    const obj = e.getData().toObject()

    obj.t = e.begin()

    return obj
  })
}

module.exports = async (...args) => {
  try {
    return await rollupDatapoints(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
