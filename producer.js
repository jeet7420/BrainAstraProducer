const WebSocket = require('ws');
const { Kafka } = require('kafkajs');
 
const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', ws => {
    console.log('Server Side Connection Established');
  ws.on('message', message => {
    console.log(`Received message => ${message}`);
    var obj = JSON.parse(message);
    var time = obj.time;
    var value = obj.value;
    var type = obj.type;
    var userName = obj.userName;
    fireEvent(time, value, type, userName);
  })
  ws.on('close', () => {
      console.log('Server Side Connection Closed');
  })
})

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ["134.209.154.124:9092"]
})

const topic = "brainco_data";
const producer = kafka.producer();

function fireEvent (time, value, type, userName) {
    const run = async () => {
        await producer.connect();
        await producer.send({
          topic,
                  messages: [
                      {
                          key: "time",
                          value: time,
                      },
                      {
                          key: "value",
                          value: value,
                      },
                      {
                          key: "type",
                          value: type,
                      },
                      {
                          key: "userName",
                          value: userName,
                      }
                  ],
        })
      }
      run().catch(console.error);
}