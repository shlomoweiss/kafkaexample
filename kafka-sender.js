const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['192.168.1.137:9092','192.168.1.137:9093','192.168.1.137:9094']
})

let producer


async function createProducer() {
  producer = kafka.producer({allowAutoTopicCreation: true})
  
  producer.on('producer.connect', () => {
    console.log('Producer connected')
  })

  producer.on('producer.disconnect', async () => {
    console.log('Producer disconnected, recreating...')
    await createProducer()
  })

  producer.on('producer.network.request_timeout', async (payload) => {
    console.error('Producer network request timeout', payload)
    await producer.disconnect()
    await createProducer()
  })

  await producer.connect()
}


async function sendMessage(message) {
  try {
    const randomNumber = Math.floor(Math.random() * 300) + 1;
    await producer.send({
      topic: 'test-topic'+randomNumber.toString(),
      messages: [{ value: message }]
    })
    console.log("send message: "+ message + " topic"+randomNumber.toString())
  } catch (error) {
    console.error('Error producing:', error)
    // Optionally, you might want to retry sending the message
  }
}

async function run() {
  await createProducer()
  

  // Example: Send a message every 5 seconds
  setInterval(() => {
    sendMessage('Hello KafkaJS ' + new Date().toISOString())
  }, 500)
}

// Error handling for unhandled rejections and exceptions
const errorTypes = ['unhandledRejection', 'uncaughtException']

errorTypes.forEach(type => {
  process.on(type, async (e) => {
    console.log(`process.on ${type}`)
    console.error(e)
    // Instead of exiting, we'll try to recreate the producer and consumer
    if (producer) await producer.disconnect()
    
    await createProducer()
   
  })
})

run().catch(console.error)