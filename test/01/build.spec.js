/**
 * Tests for build tasks
 */

describe('build tasks', function () {
  this.timeout(120000)

  const now = new Date()
  const model = {
    props: {},
    state: {
      _id: 'taskMachine-build-current',
      source_defaults: {
        some_default: 'default'
      },
      sources: [
        {
          description: 'Build aggregates based on a method',
          queue_group: 'dendra.aggregateBuild.v1',
          sub_options: {
            ack_wait: 3600000,
            durable_name: '20181223'
          },
          sub_to_subject: 'dendra.aggregateBuild.v1.req'
        }
      ],
      created_at: now,
      updated_at: now
    }
  }

  const requestSubject = 'dendra.aggregateBuild.v1.req'

  const rollupDatapointsReq = {
    _id: 'rollup-datapoints-HASH1-HASH2-HASH3',
    auth_info: {
      jwt: 'TOKEN'
    },
    method: 'rollupDatapoints',
    spec: {
      big_math: true,
      query: {
        datastream_id: '5ae879c9fe27f428ce102df9'
      },
      rollups: [
        {
          aggregations: [
            {
              alias: 'v_sum',
              field: 'v',
              // filter: 'optional',
              func: 'sum'
            },
            {
              alias: 'v_stdev',
              args: ['biased'],
              field: 'v',
              filter: 'zeroMissing',
              func: 'std'
            },
            {
              alias: 'v_count',
              field: 'v',
              // filter: 'optional',
              func: 'count'
            }
          ],
          window: '1_d'
        }
      ],
      shift: 'so_M',
      time_cursor: '12_d',
      time_gte: '2017-01-01',
      time_lt: '2017-04-01'
      // transform: 'expression (optional)'
    }
  }

  Object.defineProperty(model, '$app', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: main.app
  })
  Object.defineProperty(model, 'key', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: 'build'
  })
  Object.defineProperty(model, 'private', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: {}
  })

  let tasks
  let machine

  after(function () {
    return Promise.all([
      model.private.stan ? new Promise((resolve, reject) => {
        model.private.stan.removeAllListeners()
        model.private.stan.once('close', resolve)
        model.private.stan.once('error', reject)
        model.private.stan.close()
      }) : Promise.resolve()
    ])
  })

  it('should import', function () {
    tasks = require('../../dist').build

    expect(tasks).to.have.property('sources')
  })

  it('should create machine', function () {
    machine = new tm.TaskMachine(model, tasks, {
      helpers: {
        logger: console
      },
      interval: 500
    })

    expect(machine).to.have.property('model')
  })

  it('should run', function () {
    model.scratch = {}

    return machine.clear().start().then(success => {
      /* eslint-disable no-unused-expressions */
      expect(success).to.be.true

      // Verify task state
      expect(model).to.have.property('sourcesReady', true)
      expect(model).to.have.property('stanCheckReady', false)
      expect(model).to.have.property('stanCloseReady', false)
      expect(model).to.have.property('stanReady', true)
      expect(model).to.have.property('subscriptionsReady', true)
      expect(model).to.have.property('versionTsReady', false)

      // Check for defaults
      expect(model).to.have.nested.property('sources.dendra_aggregateBuild_v1_req.some_default', 'default')
    })
  })

  it('should process rollupDatapoints request', function () {
    const msgStr = JSON.stringify(rollupDatapointsReq)

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) => err ? reject(err) : resolve(guid))
    })
  })

  it('should wait for 60 seconds', function () {
    return new Promise(resolve => setTimeout(resolve, 60000))
  })
})
