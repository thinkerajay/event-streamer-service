import {Consumer, Kafka} from "kafkajs";
import KafkaConnection from "./KafkaConnection";
import {Socket} from "socket.io";
import {logger} from "../config/logger";


export interface PullStart {
    topics: string[],
    clientName: string
}

export default class KafkaConsumer {
    private kafka: Kafka;
    private topics: string[];
    private consumer: Consumer;
    private clientName: string;

    constructor() {
        this.kafka = KafkaConnection.getInstance().kafka;
        this.consumer = this.kafka.consumer({
            groupId: 'event-streamer-group'
        });
    }

    async disconnect() {
        await this.consumer.disconnect()
    }

    async connectAndSubscribe(data: string) {
        const eventData: PullStart = JSON.parse(data)
        await this.consumer.connect()
        this.topics = eventData.topics;
        this.clientName = eventData.clientName;
        logger.info(`Received consumer topics %o`, eventData.topics)
        for (const topic of eventData.topics) {
            await this.consumer.subscribe({
                topic,
                fromBeginning: true
            })
            logger.info(`%s subscribed to %s topic`, this.clientName, topic)
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
    }

    async pushEventsToClient(socket: Socket) {
        logger.info(`Pushing events to client %s`, this.clientName)
        await this.consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                logger.info(`Pushing event to client %s`, this.clientName)
                socket.emit('events', JSON.stringify(message))
            },
        })
    }
}