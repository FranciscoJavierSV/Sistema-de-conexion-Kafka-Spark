require('dotenv').config();
const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');

const brokers = process.env.BROKERS
  ? process.env.BROKERS.split(',').map((b) => b.trim())
  : ['100.100.10.100:9094', '100.100.10.100:9096', '100.100.10.100:9098'];

const kafka = new Kafka({
  clientId: 'proyecto-consumer',
  brokers,
});

const consumer = kafka.consumer({ groupId: 'grupo-examen-final' });
const outputDir = path.resolve(__dirname, 'consumer_output');

const sanitize = (value) => String(value || 'unknown').replace(/[^a-zA-Z0-9_-]/g, '_');

if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

const recibir = async () => {
  await consumer.connect();
  // Asegúrate de haber creado el tópico 'ventas' antes de correr esto
  await consumer.subscribe({ topic: 'ventas', fromBeginning: false });

  console.log('Consumidor escuchando en el clúster distribuido...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawValue = message.value.toString();
      let data;
      try {
        data = JSON.parse(rawValue);
      } catch (error) {
        console.error('Error al parsear mensaje:', error.message);
        return;
      }
      const machineName = sanitize(data.maquina || `partition_${partition}`);
      const filePath = path.join(outputDir, `${machineName}.jsonl`);
      const record = {
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        ...data,
      };
      fs.appendFileSync(filePath, JSON.stringify(record) + '\n');
      console.log(`[${machineName}] part=${partition} offset=${message.offset} tx=${data.id_transaccion} suc=${data.sucursal}`);
    },
  });
};

recibir().catch(console.error);