const { Kafka,logLevel } = require('kafkajs');


const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['192.168.1.137:9092','192.168.1.137:9093','192.168.1.137:9094'],
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 10,
    retries: 2
  }
})


let consumer
let lastHeartbeat = Date.now();
let timerid,unrgistercrash,unregisterdisconnet,unregisterconnect,unregisterhb

console.log("first hb is "+ lastHeartbeat)


async function  timerCallback() {
  curnetTS=Date.now()
  timepass = (curnetTS-lastHeartbeat)/1000
  if (timepass > 15 ){
    console.log("to maush time pass stope consumer&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    consumeroff ();
    await consumer.disconnect()
    await consumer.stop() 
    await createConsumer()
    
  } else{
      timerid=setTimeout(timerCallback, 10000);
  }  
}

  function consumeroff (){
    unregisterconnect()
    unregisterdisconnet()
    unregisterhb()
    unrgistercrash()

  }


  async function createConsumer() {
    let randomInt = Math.floor(Math.random() * 100000000) + 1;
    let groupid =   'test-group' + randomInt
    console.log("craeat consumer with grpt id:" + groupid)
    
    consumer = kafka.consumer({ groupId: groupid })
  
  unregisterconnect=consumer.on('consumer.connect', () => {
    console.log('Consumer connected')
  })

  unregisterdisconnet=consumer.on('consumer.disconnect', async () => {
    console.log('@@@@@@@@@@@@@@@@Consumer disconnected, recreating...')
    if (timerid)
    {
      clearTimeout(timerid);
    }
    consumeroff ()
    await consumer.stop()
    await createConsumer()
  })

  

  unregisterhb= consumer.on('consumer.heartbeat', async () => {
    let Currnthb = Date.now()
    let timepass = (Currnthb-lastHeartbeat)/1000
    console.log('&&&&&&got hb is '+ timepass)
    lastHeartbeat = Currnthb
  })

  unrgistercrash =consumer.on('consumer.crash', async (error) => {
    console.error('@@@@@@@@@@@@@@@@@Consumer crashed', error)
    if (timerid)
      {
        clearTimeout(timerid);
      }
    consumeroff ()  
    await consumer.disconnect()
    await consumer.stop()
    await createConsumer()
  })

  

  await consumer.connect()
  let topics =[]
  for (let i = 1; i <= 100; i++) {
    topics.push('test-topic'+i.toString())
     //await consumer.subscribe({ topic: 'test-topic'+i.toString(), fromBeginning: true })
  }
  await consumer.subscribe({ topics: topics, fromBeginning: true })
  timerid = setTimeout(timerCallback, 60000);  
  try{
   await consumer.run({
     eachMessage: async ({ topic, partition, message }) => {
       console.log({
         topic,
         partition,
         offset: message.offset,
         value: message.value.toString(),
       })
     },
   })
  }catch(e)
  {
    console.log(`@@@@@@@@@@@@@@@@@@@@@@@main consumer run got exception`)
    console.error(e)
    consumeroff ()
    await consumer.disconnect()
    await consumer.stop()
    await createConsumer()
  }

} 


async function run() {
  
  await createConsumer()

 
}

// Error handling for unhandled rejections and exceptions
const errorTypes = ['unhandledRejection', 'uncaughtException']

errorTypes.forEach(type => {
  process.on(type, async (e) => {
    console.log(`@@@@@@@@@@@@@@@@@@@@@@@process.on ${type}`)
    console.error(e)
    // Instead of exiting, we'll try to recreate the producer and consumer
    if (timerid)
      {
        clearTimeout(timerid);
      }
    if (consumer){
       await consumer.disconnect()
       await consumer.stop()
      consumeroff ()
    }
     await createConsumer()
  })
})

run().catch(console.error)