require('dotenv').config();
const { Kafka } = require('kafkajs');
const { faker } = require('@faker-js/faker');

const brokers = process.env.BROKERS
  ? process.env.BROKERS.split(',').map((b) => b.trim())
  : ['100.100.10.100:9094', '100.100.10.101:9096', '100.100.10.102:9098'];

const kafka = new Kafka({
  clientId: 'proyecto-producer',
  brokers,
});

const producer = kafka.producer();
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


const generarData = () => ({
  id_transaccion: faker.string.uuid(),
  monto: parseFloat(faker.commerce.price()),
  sucursal: faker.location.city(),
  cliente: faker.person.fullName(),
  producto: faker.commerce.product(),
  cantidad: faker.number.int({ min: 1, max: 20 }),
  moneda: 'MXN',
  pais: faker.location.country(),
  metodo_pago: faker.helpers.arrayElement(['tarjeta', 'efectivo', 'paypal', 'transferencia']),
  fecha: faker.date.recent({ days: 30 }).toISOString(),
  maquina: faker.helpers.arrayElement(['kafka1', 'kafka2', 'kafka3']),
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