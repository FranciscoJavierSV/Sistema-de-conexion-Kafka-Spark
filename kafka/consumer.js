const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const brokers = [
  '100.100.10.100:9094',
  '100.100.10.101:9096',
  '100.100.10.102:9098'
];

const kafka = new Kafka({
  clientId: 'proyecto-consumer',
  brokers,
});

const consumer = kafka.consumer({
  groupId: 'grupo-examen-final',
});

const mongoClient = new MongoClient('mongodb://localhost:27017');

const TOPICOS = [
  'ventas_json',
  'inventario_csv',
  'clientes_sql',
  'logs',
  'metricas'
];

let totalMensajes = 0;

const recibir = async () => {
  // Conectar a MongoDB
  await mongoClient.connect();

  const db = mongoClient.db('kafka_db');

  console.log('=================================');
  console.log('MongoDB conectado');
  console.log('Base de datos: kafka_db');
  console.log('=================================');

  // Conectar a Kafka
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
        timestamp: new Date(Number(message.timestamp)),
        data: contenido,
        fechaInsercion: new Date(),
      };

      // Guarda cada tópico en su propia colección
      const collection = db.collection(topic);

      await collection.insertOne(registro);

      if (totalMensajes % 1000 === 0) {
        console.log(
          `[${new Date().toLocaleTimeString()}] Consumidos: ${totalMensajes}`
        );
      }
    },
  });
};

recibir().catch((err) => {
  console.error('Error en consumer:', err);
});