const { Kafka } = require('kafkajs');

// Configuration de Kafka
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

// Création d'un producteur Kafka
const producer = kafka.producer();

// Fonction pour envoyer un message à un topic Kafka
const sendMessage = async (topic, message) => {
    try {
        // Connexion au producteur Kafka 
        await producer.connect();

        // Envoi du message au topic spécifié
        await producer.send({ 
            topic,
            messages: [{ value: JSON.stringify(message) }],
        });

        // Déconnexion du producteur Kafka après l'envoi
        await producer.disconnect();
    } catch (error) {
        console.error('Erreur lors de l\'envoi du message à Kafka:', error);
    }
};

// Fonction pour consommer les messages d'un topic Kafka
const consumeMessages = async (topic) => {
    const consumer = kafka.consumer({ groupId: 'my-group' });

    // Connecter le consommateur Kafka
    await consumer.connect();

    // Souscrire au topic spécifié
    await consumer.subscribe({ topic });

    // Écouter les messages entrants
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            // Traiter le message reçu
            console.log({
                value: message.value.toString(),
            });
        },
    });
};

module.exports = {
    sendMessage,
    consumeMessages,
};
