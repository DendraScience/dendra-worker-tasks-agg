/**
 * Process an individual message.
 */

const methods = require('./methods')

async function processItem (
  { data, dataObj, msgSeq },
  ctx) {
  const { documentService, logger, subSubject } = ctx

  /*
    Validate aggregation method.
   */

  if (!dataObj.method) throw new Error('Aggregate method undefined')

  const method = methods[dataObj.method]

  if (!method) throw new Error('Aggregate method not supported')

  /*
    Invoke aggregation method.
   */

  const startedAt = new Date()
  const aggRes = await method(dataObj, ctx)
  const finishedAt = new Date()

  if (!aggRes) throw new Error('Aggregate result undefined')

  logger.info('Built', { msgSeq, subSubject, startedAt, finishedAt })

  const { _id: aggDocId } = dataObj
  if (typeof aggDocId !== 'string') throw new Error('Invalid aggregate _id')

  /*
    Create document in aggregate store.
   */

  // Redact auth tokens
  delete dataObj.auth_info

  const doc = await documentService.create({
    _id: aggDocId,
    content: {
      request: dataObj,
      result: {
        build_info: {
          duration: finishedAt - startedAt,
          started_at: startedAt,
          finished_at: finishedAt
        },
        data: aggRes
      }
    }
  })

  logger.info('Stored', { msgSeq, subSubject, _id: doc._id })
}

module.exports = processItem
