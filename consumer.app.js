
const kafka = require('kafka-node');


try {
  const kafkaHost = "10.100.102.156:9092";
  console.log (kafkaHost);
  var options = {
    kafkaHost: kafkaHost, // connect directly to kafka broker (instantiates a KafkaClient)
    batch: undefined, // put client batch settings if you need them
    groupId: 'ExampleTestGroup3',
    sessionTimeout: 15000,
    autoCommit: false,
    // An array of partition assignment protocols ordered by preference.
    // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
    protocol: ['roundrobin'],
    encoding: 'utf8', // default is utf8, use 'buffer' for binary data
   
    // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
    // equivalent to Java client's auto.offset.reset
    fromOffset: 'latest', // default
    commitOffsetsOnFirstJoin: false, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
    // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
    outOfRangeOffset: 'latest', // default
    // Callback to allow consumers with autoCommit false a chance to commit before a rebalance finishes
    // isAlreadyMember will be false on the first connection, and true on rebalances triggered after that
    onRebalance: (isAlreadyMember, callback) => { callback(); } // or null
  };

  console.log("grup id:"+ options.groupId);
  var consumerGroup ;
  //consumerGroup.pause();
  //consumerGroup.setOffset("test1", 0, 0);
  //consumerGroup.resume();
 
  function init() {
    console.log("ini init=========");
    consumerGroup = new kafka.ConsumerGroup(options, 'test2');
    consumerGroup.on('message', function(message) {
  
      console.log(
        'kafka-> ',
        JSON.stringify(message)
      );
      this.commit(false,error=>{console.log(" commit +" ,error);});
    });
      consumerGroup.on('error', function(err) {
         console.log('error^^^^^^^^^^^^^^^^^^^^^^', err);
         consumerGroup.generationId = null;
         consumerGroup.close(false,function (){})
         init();
        
      });  
      

  }
  init();


}catch(e) {
  init()
}  