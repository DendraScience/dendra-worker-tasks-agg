/**
 * Init version timestamp in model.
 */

module.exports = {
  clear (m) {
    m.versionTs = m.state.created_at
  },

  guard (m) { return false }, // Never run

  execute () {}
}
