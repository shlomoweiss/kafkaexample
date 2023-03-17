
const kafka = require('kafka-node');
const config = require('./config');

try {
  //const kafkaHost = process.env.KAFKA_HOST;
  //console.log (kafkaHost);
  const Producer = kafka.Producer;
  var client;
  var producer ;
  const kafka_topic = 'test2';
  console.log(kafka_topic);
  let payloads = [
    {
      topic: kafka_topic,
      messages: config.kafka_topic
    }
  ];

  const closeCB = function (){
    init();    
  }

  const readyCB = function (){
    console.log("broker ready");
    setInterval(request, 1000);

  };

  const  errorCB= function(err){
    console.log(err);
    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
    producer.close(closeCB)
  }

  function init(){
    client = new kafka.KafkaClient({kafkaHost:"10.100.102.156:9092"});
    producer = new Producer(client);
    producer.on('ready', readyCB);
    producer.on('error',errorCB);

  }


  
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
  
  

  init();
}
catch(e) {
  console.log(e);
}
