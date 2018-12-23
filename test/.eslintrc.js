// SEE: http://eslint.org/docs/user-guide/configuring
module.exports = {
  env: {
    'mocha': true,
    'node': true
  },
  extends: 'standard',
  globals: {
    app: true,
    assert: true,
    expect: true,
    helper: true,
    main: true,
    path: true,
    tm: true
  },
  root: true,
  parserOptions: {
    sourceType: 'module'
  }
}
