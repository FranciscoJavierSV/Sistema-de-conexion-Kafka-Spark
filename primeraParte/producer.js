const { Kafka } = require('kafkajs');
const { faker } = require('@faker-js/faker');

const kafka = new Kafka({
  clientId: 'proyecto-producer',
  brokers: ['100.100.10.100:9094', '100.100.10.100:9096', '100.100.10.100:9098']
});

const producer = kafka.producer();
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


const generarData = () => ({
  id_transaccion: faker.string.uuid(),
  monto: parseFloat(faker.commerce.price()),
  sucursal: faker.location.city(),
});

const enviarCarga = async () => {
  await producer.connect();
  console.log('Iniciando envío');

  const total = 1000000;
  const batchSize = 1000; // Lotes más pequeños para ver el flujo mejor

  for (let i = 0; i < total / batchSize; i++) {
    const mensajes = Array.from({ length: batchSize }, () => ({
      // Agregamos una KEY aleatoria para FORZAR que se intercalen en las particiones
      key: faker.string.uuid(), 
      value: JSON.stringify(generarData())
    }));

    await producer.send({
      topic: 'ventas',
      messages: mensajes,
    });

    console.log(`Lote ${i + 1} enviado (${(i + 1) * batchSize} total)`);
  }

  await producer.disconnect();
};

enviarCarga().catch(console.error);