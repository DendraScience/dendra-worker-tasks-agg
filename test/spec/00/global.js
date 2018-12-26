const chai = require('chai')
const feathers = require('feathers')
const restClient = require('feathers-rest/client')
const request = require('request')
const app = feathers()

const tm = require('@dendra-science/task-machine')
tm.configure({
  // logger: console
})

app.logger = console

const AGGREGATE_JSON_API_URL = 'http://localhost:3036'

const WEB_API_URL = 'http://api.dendra.science/v1'

app.set('connections', {
  aggregateStore: {
    app: feathers().configure(restClient(AGGREGATE_JSON_API_URL).request(request))
  },
  web: {
    app: feathers().configure(restClient(WEB_API_URL).request(request))
  }
})

app.set('clients', {
  stan: {
    client: 'test-agg-{key}',
    cluster: 'test-cluster',
    opts: {
      uri: 'http://localhost:4222'
    }
  }
})

global.assert = chai.assert
global.expect = chai.expect
global.main = {
  app
}
global.tm = tm
