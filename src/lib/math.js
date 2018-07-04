/**
 * Math.js configured and exported.
 */

const math = require('mathjs')

math.config({
  number: 'BigNumber',
  precision: 10 // Hardcoded!
})

module.exports = math
