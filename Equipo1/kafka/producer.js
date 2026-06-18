require('dotenv').config();
const { Kafka } = require('kafkajs');
const { faker } = require('@faker-js/faker');

const brokers = process.env.BROKERS
  ? process.env.BROKERS.split(',').map(b => b.trim())
  : [
      '100.100.10.100:9094',
      '100.100.10.101:9096',
      '100.100.10.102:9098'
    ];

const kafka = new Kafka({
  clientId: 'proyecto-producer',
  brokers,
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const producer = kafka.producer();

const TOTAL_REGISTROS = 100000;
const BATCH_SIZE = 1000;

function generarJSON() {
  return {
    id_transaccion: faker.string.uuid(),
    monto: parseFloat(faker.commerce.price()),
    sucursal: faker.location.city(),
    cliente: faker.person.fullName(),
    producto: faker.commerce.product(),
    cantidad: faker.number.int({ min: 1, max: 20 }),
    moneda: 'MXN',
    pais: faker.location.country(),
    metodo_pago: faker.helpers.arrayElement([
      'tarjeta',
      'efectivo',
      'paypal',
      'transferencia'
    ]),
    fecha: faker.date.recent({ days: 30 }).toISOString(),
    maquina: faker.helpers.arrayElement([
      'kafka1',
      'kafka2',
      'kafka3'
    ])
  };
}

function generarCSV() {
  return [
    faker.string.uuid(),
    faker.commerce.product(),
    faker.number.int({ min: 1, max: 100 }),
    faker.commerce.department(),
    faker.company.name(),
    faker.location.city(),
    faker.location.country(),
    faker.number.float({ min: 1, max: 1000 }),
    faker.date.recent().toISOString(),
    faker.person.fullName()
  ].join(',');
}

function generarSQL() {
  return {
    id_cliente: faker.string.uuid(),
    nombre: faker.person.firstName(),
    apellido: faker.person.lastName(),
    correo: faker.internet.email(),
    telefono: faker.phone.number(),
    direccion: faker.location.streetAddress(),
    ciudad: faker.location.city(),
    pais: faker.location.country(),
    fecha_registro: faker.date.past().toISOString(),
    estado: faker.helpers.arrayElement([
      'ACTIVO',
      'INACTIVO'
    ])
  };
}

function generarLog() {
  return {
    timestamp: new Date().toISOString(),
    nivel: faker.helpers.arrayElement([
      'INFO',
      'WARN',
      'ERROR'
    ]),
    servicio: faker.helpers.arrayElement([
      'API',
      'AUTH',
      'DB'
    ]),
    mensaje: faker.lorem.sentence()
  };
}

function generarMetrica() {
  return {
    cpu: faker.number.float({ min: 0, max: 100 }),
    memoria: faker.number.float({ min: 0, max: 100 }),
    disco: faker.number.float({ min: 0, max: 100 }),
    timestamp: new Date().toISOString()
  };
}

async function enviarLote(topic, generador) {
  const mensajes = Array.from(
    { length: BATCH_SIZE },
    () => ({
      key: faker.string.uuid(),
      value: JSON.stringify(generador())
    })
  );

  await producer.send({
    topic,
    acks: -1,
    messages: mensajes
  });
}

async function main() {
  await producer.connect();

  console.log('Producer conectado');

  const inicio = Date.now();

  for (let i = 0; i < TOTAL_REGISTROS / BATCH_SIZE; i++) {

    await enviarLote('ventas_json', generarJSON);

    await enviarLote('inventario_csv', () => ({
      csv: generarCSV()
    }));

    await enviarLote('clientes_sql', generarSQL);

    await enviarLote('logs', generarLog);

    await enviarLote('metricas', generarMetrica);

    console.log(
      `Lote ${i + 1} enviado (${(i + 1) * BATCH_SIZE} registros por tópico)`
    );
  }

  const fin = Date.now();

  console.log(
    `Tiempo total: ${((fin - inicio) / 1000).toFixed(2)} segundos`
  );

  await producer.disconnect();
}

main().catch(console.error);