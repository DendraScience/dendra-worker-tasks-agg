/**
 * Process an individual message.
 */

const methods = require('./methods')

async function processItem (
  { data, dataObj, msgSeq },
  ctx) {
  const { documentService, logger, subSubject } = ctx
  try {
    /*
      Validate build method.
     */

    if (!dataObj.method) throw new Error('Build method undefined')

    const method = methods[dataObj.method]

    if (!method) throw new Error('Build method not supported')

    /*
      Invoke build method.
     */

    const startedAt = new Date()
    const buildRes = await method(dataObj, ctx)
    const finishedAt = new Date()

    if (!buildRes) throw new Error('Build result undefined')

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
          data: buildRes
        }
      }
    })

    logger.info('Stored', { msgSeq, subSubject, _id: doc._id })
  } catch (err) {
    logger.error('Processing error', { msgSeq, subSubject, err, dataObj })
  }
}

module.exports = processItem
