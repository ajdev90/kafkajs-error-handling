const { Kafka, logLevel } = require('kafkajs')
const consts = require('./constants')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${consts.KAFKA_HOST_PORT}`],
  clientId: 'new_consumer',
  retry: {
    retries: 5, //Max number of retries per call
    factor: .2, //Randomization factor
    initialRetryTime: 300, //Initial value used to calculate the retry in milliseconds
    maxRetryTime: 30000, //Maximum wait time for a retry in milliseconds
    multiplier: 2, //Exponential factor
    restartOnFailure: async (error) => {
      console.log(`error=${error}`,error);
      return true;
    } // only used for consumer
  }
})

const topic = 'new_topic'
const consumer = kafka.consumer({ groupId: 'new_group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic})
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
      
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
