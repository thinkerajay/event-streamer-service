import {Kafka} from "kafkajs";

export default class KafkaConnection {
    private static instance: KafkaConnection;
    kafka: Kafka;

    private constructor() {
        this.kafka = new Kafka({
            clientId: 'event-streamer-service',
            brokers: ['localhost:9092'],

        });
    }


    public static getInstance(): KafkaConnection {
        // if (!KafkaConnection.instance) {
        //     KafkaConnection.instance = new KafkaConnection();
        // }

        return new KafkaConnection();
    }

}