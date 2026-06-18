require('dotenv').config();
const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');

const brokers = process.env.BROKERS
  ? process.env.BROKERS.split(',').map((b) => b.trim())
  : [
      '100.100.10.103:9094',
      '100.100.10.104:9096',
      '100.100.10.105:9098'
    ];

const kafka = new Kafka({
  clientId: 'proyecto-consumer',
  brokers,
});

const consumer = kafka.consumer({
  groupId: 'grupo-examen-final',
});

const outputDir = path.resolve(__dirname, 'consumer_output');

if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

const outputFile = path.join(
  outputDir,
  'kafka_consumido.jsonl'
);

let totalMensajes = 0;

const TOPICOS = [
  'ventas_json',
  'inventario_csv',
  'clientes_sql',
  'logs',
  'metricas'
];

const recibir = async () => {
  await consumer.connect();

  for (const topic of TOPICOS) {
    await consumer.subscribe({
      topic,
      fromBeginning: false,
    });
  }

  console.log('=================================');
  console.log('Consumer conectado al clúster');
  console.log('Tópicos:', TOPICOS.join(', '));
  console.log('=================================');

  await consumer.run({
    eachMessage: async ({
      topic,
      partition,
      message,
    }) => {

      totalMensajes++;

      const rawValue = message.value.toString();

      let contenido;

      try {
        contenido = JSON.parse(rawValue);
      } catch {
        contenido = rawValue;
      }

      const registro = {
        topic,
        partition,
        offset: Number(message.offset),
        timestamp: message.timestamp,
        data: contenido,
      };

      fs.appendFileSync(
        outputFile,
        JSON.stringify(registro) + '\n'
      );

      if (totalMensajes % 1000 === 0) {
        console.log(
          `[${new Date().toLocaleTimeString()}] ` +
          `Consumidos: ${totalMensajes}`
        );
      }
    },
  });
};

recibir().catch((err) => {
  console.error('Error en consumer:', err);
});