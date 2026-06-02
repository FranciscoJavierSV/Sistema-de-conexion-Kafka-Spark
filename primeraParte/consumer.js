const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'proyecto-consumer',
  brokers: ['100.100.10.100:9094', '100.100.10.100:9096', '100.100.10.100:9098']
});

const consumer = kafka.consumer({ groupId: 'grupo-examen-final' });

const recibir = async () => {
  await consumer.connect();
  // Asegúrate de haber creado el tópico 'ventas' antes de correr esto
  await consumer.subscribe({ topic: 'ventas', fromBeginning: true });

  console.log('Consumidor escuchando en el clúster distribuido...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      
      // Mostramos la partición para validar la distribución de carga entre los 3 nodos
      console.log(`[PARTICIÓN-${partition}] | TX: ${data.id_transaccion} | Sucursal: ${data.sucursal} | $${data.monto}`);
    },
  });
};

recibir().catch(console.error);