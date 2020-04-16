// Kafka setup
kafka = require('kafka-node'),
client =  new kafka.KafkaClient();
Consumer = kafka.Consumer,

// database
mongojs = require('mongojs'),
db = mongojs(process.env.MONGO_URL || 'localhost:27017/employeeTest');


var consumer = new Consumer(
    client,
    [{ topic: 'employeeTopic', partition: 0 }],
    {fromOffset: true}
    );

consumer.on('ready', function() {
console.log('KAFKA consumer ready');
});

consumer.on('message', function (message) {
    console.log("Message is",message);
    // example message: {"topic":"my-node-topic","value":"{\"timestamp\":1425599538}","offset":0,"partition":0,"key":{"type":"Buffer","data":[]}}
    db.collection('employeeData').insert(JSON.parse(message.value), function(err, data) {
        if(err) {
        console.log('MONGO error updating document: ' + err);
        } else {
        console.log('MONGO updating document OK:' + JSON.parse(message.value));
        }
    });
});

consumer.on('error', function (err) {
console.log('KAFKA consumer error:' + err);
});