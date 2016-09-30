const azure = require('azure')
const Promise = require('bluebird')

let queue_service
/*
Usage: require(node-queue-adapten)('http://my-azure-url')
*/
module.exports = function (queueConnectionString, logLevel = 'TRACE') {
  queue_service = azure.createServiceBusService(queueConnectionString)
  console.log('created service')
  queue_service.logger = new azure.Logger(azure.Logger.LogLevels[logLevel])

  return {
    /*
    * Creates a queue if it does not already exist
    */
    createQueueIfNotExists: Promise.promisify(
      queue_service.createQueueIfNotExists,
      { context: queue_service }
    ),

    /*
    * Sends a message to the given queue
    */
    sendQueueMessage: function (queue_name, message) {
      if (typeof message === 'object') {
        message = JSON.stringify(message)
      }
      return new Promise(function (resolve, reject) {
        let queue_message = {
          body: message
        }
        queue_service.sendQueueMessage(queue_name, queue_message, function (err) {
          if (err) reject(err)
          else resolve()
        })
      })
    },

    /*
    * Reads a message from a subscription. Peek lock is used, which leaves the message
    * undeleted until it is explicitly done.
    */
    readMessageFromQueue: function (queue_name) {
      return new Promise(function (resolve, reject) {
        let options = {
          isPeekLock: true
        }
        queue_service.receiveQueueMessage(queue_name, options, function (err, msg) {
          if (err) {
            if (err === 'No messages to receive') {
              // Handle this by switching message queues in the consumer
              resolve(null)
            } else {
              reject(err)
            }
          }
          else resolve(msg)
        })
      })
    },

    /*
    * Deletes a message from a subscription
    */
    deleteMessageFromQueue: Promise.promisify(
      queue_service.deleteMessage,
      { context: queue_service }
    ),

    /*
    * Unlocks a locked message in a queue. Useful for when the message could not be processed.
    */
    unlockMessageInQueue: Promise.promisify(
      queue_service.unlockMessage,
      { context: queue_service }
  )}
}
