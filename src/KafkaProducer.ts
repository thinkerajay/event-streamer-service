import {Kafka, Producer} from "kafkajs";
import KafkaConnection from "./KafkaConnection";
import {logger} from "../config/logger";

export interface PushStart {
    topic: string,
    clientName: string
}

export interface PushEvent {
    topic: string,
    type: string,
    payload: object

}

export default class KafkaProducer {
    private kafka: Kafka;
    private topic: string;
    private producer: Producer;
    private clientName: string;

    constructor() {
        this.kafka = KafkaConnection.getInstance().kafka;
        this.producer = this.kafka.producer();
    }

    async disconnect() {
        await this.producer.disconnect()
    }

    async createTopic(data: string) {
        const eventData: PushStart = JSON.parse(data)
        logger.info(`Received topic data %o`, eventData);
        const kafkaAdmin = this.kafka.admin();
        this.topic = eventData.topic;
        this.clientName = eventData.clientName;

        await kafkaAdmin.connect()

        const allTopics: string[] = await kafkaAdmin.listTopics()
        if (allTopics.includes(eventData.topic)) {
            logger.info(`Topic %s already exists `, eventData.topic);
            return;
        }

        // creating topics
        logger.info('creating topics')
        await kafkaAdmin.createTopics({
            topics: [{
                topic: eventData.topic,
                numPartitions: 1,
                replicationFactor: 1,
            }]
        })
        logger.info('created topics successfully')
    }

    async pushEvents(data: string) {
        await this.producer.connect()

        const eventData: PushEvent = JSON.parse(data)
        if (this.topic !== eventData.topic) {
            logger.warn('Cannot push to %o topic', eventData.topic)
            return;
        }
        logger.info('Pushing events from producer ...')
        const jsonData = {
            clientName: this.clientName,
            type: eventData.type,
            ...eventData.payload
        }

        await this.producer.send({
            topic: eventData.topic,
            messages: [{
                value: JSON.stringify(jsonData)
            }]
        })

    }


}