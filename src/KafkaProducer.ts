import {Kafka, Producer} from "kafkajs";
import KafkaConnection from "./KafkaConnection";
import {logger} from "../config/logger";
import {socketClientNameMap} from "./MessageHandlers";
import {Socket} from "socket.io";

export interface PushStart {
    topic: string,
    clientName: string
}

export interface PushEvent {
    topic: string,
    type: string,
    payload: object

}

export interface EventWithAvgCal {
    topic: string,
    clientName: string,
    pushToClientName: string,
    keys: string[],
    window: number,
    type: string,
    payload: object
}

export default class KafkaProducer {
    private kafka: Kafka;
    private topic: string;
    private producer: Producer;
    private clientName: string;
    private window: number;
    private events: object[];

    constructor() {
        this.kafka = KafkaConnection.getInstance().kafka;
        this.producer = this.kafka.producer();
        this.window = 6 * 1000;
        this.events = [];
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
        logger.info('creating topic %s', this.topic)
        await kafkaAdmin.createTopics({
            topics: [{
                topic: eventData.topic,
                numPartitions: 1,
                replicationFactor: 1,
            }]
        })
        logger.info('created topics successfully')
    }

    startLoop(data: string) {
        const eventDataObj: EventWithAvgCal = JSON.parse(data);
        setInterval(() => {
            const eventData: EventWithAvgCal = JSON.parse(data);
            logger.info(`Starting push event with avg cal loop %o, %s....`, eventData.keys, typeof eventData.keys)
            const keyVals = new Map<string, number>();
            if (!this.events) {
                logger.info(`no events for avg calculation yet....`)
                return;
            }
            for (const event of this.events) {
                const eventObj: { [index: string]: any } = event;
                for (const key of ['cpu', 'memory', 'disk']) {
                    if (eventObj[key]) {
                        keyVals.set(key, (Number(keyVals.get(key)) || 0) + (Number(eventObj[key]) || 0));
                    }
                }
            }
            const sendSocket: Socket = socketClientNameMap.get(eventData.clientName);
            logger.info(`Sending Avg Calculated events of length %o.... `, keyVals);
            sendSocket.emit('events_with_avg_cal', JSON.stringify(Object.fromEntries(keyVals)));
            // reset events array
            this.events = [];

        }, eventDataObj.window || this.window);
    }

    async pushEventsWithAvgCal(data: string) {
        await this.producer.connect()

        const eventData: EventWithAvgCal = JSON.parse(data)
        this.events.push(eventData.payload);


        logger.info('Pushing events with average calculation from producer ...')
        const jsonData = {
            clientName: this.clientName,
            type: eventData.type,
            payload: eventData.payload
        }

        await this.producer.send({
            topic: eventData.topic,
            messages: [{
                value: JSON.stringify(jsonData)
            }]
        })

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
            payload: eventData.payload
        }

        await this.producer.send({
            topic: eventData.topic,
            messages: [{
                value: JSON.stringify(jsonData)
            }]
        })

    }


}