
const kafka = require('kafka-node');
const config = require('./config');

try {
  //const kafkaHost = process.env.KAFKA_HOST;
  //console.log (kafkaHost);
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient({kafkaHost:"192.168.1.44:30036"});
  const producer = new Producer(client);
  const kafka_topic = 'test1';
  console.log(kafka_topic);
  let payloads = [
    {
      topic: kafka_topic,
      messages: config.kafka_topic
    }
  ];
  
  function request() {
    producer.send(payloads, (err, data) => {
      if (err) {
        console.log('[kafka-producer -> '+kafka_topic+' '+err+' ]: broker update failed');
      } else {
        console.log(JSON.stringify(data) +  ' broker update success');
       
        return
      }
    });
  }

  producer.on('ready', function() {
    console.log("broker ready");
    setInterval(request, 1000);
    
  });

  producer.on('error', function(err) {
    console.log(err);
    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
    throw err;
  });
}
catch(e) {
  console.log(e);
}
