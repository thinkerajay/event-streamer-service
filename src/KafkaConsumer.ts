import 'reflect-metadata';
import {Consumer, Kafka} from "kafkajs";
import KafkaConnection from "./KafkaConnection";
import {Socket} from "socket.io";
import {logger} from "../config/logger";
import KafkaProducer, {PushEvent} from "./KafkaProducer";
import {compact} from 'lodash';
import DbConnector from "./DbConnector";
import {Container} from "typedi";


export interface PullStart {
    topics: string[],
    clientName: string
}

export interface EventFilter {
    key: string,
    value: string,
    operation: string
}

enum Operation {
    AND,
    OR
}

export interface EventWithFilters extends PullStart {
    type: string,
    filters: EventFilter[],
    operation: Operation
}

export interface EventWithJoin extends PullStart {
    key: string,
    topic: string,
}

export default class KafkaConsumer {
    private kafka: Kafka;
    private topics: string[];
    private consumer: Consumer;
    private clientName: string;
    private dbConnector: DbConnector;

    constructor() {
        this.kafka = KafkaConnection.getInstance().kafka;
        this.dbConnector = Container.get(DbConnector);
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

    buildFilterQuery(filters: EventFilter[], operation: Operation) {
        const parameters = [];
        for (const filter of filters) {
            const key = filter.key;
            let parameter;
            switch (filter.operation) {
                case '>':
                    parameter = {key: {'$gt': filter.value}}
                    break;
                case '<':
                    parameter = {key: {'$lt': filter.value}}
                    break;
                case '==':
                    parameter = {key: filter.value}
                    break;
            }
            parameters.push(parameter);
        }
        if (operation == Operation.OR) {
            return {
                '$or': parameters
            }
        }

        return {
            '$and': parameters
        }

    }

    async pushFilteredEventsToClient(data: string, socket: Socket) {
        logger.info(`Pushing Filtered events to client %s`, this.clientName)

        const eventData: EventWithFilters = JSON.parse(data);
        const filters: EventFilter[] = eventData.filters;

        const filterQuery = this.buildFilterQuery(filters, eventData.operation);
        const cursor = await this.dbConnector.findRecords(filterQuery);

        // send all the records received till now to client
        await cursor.forEach((event) =>{
            socket.emit('filtered_events', JSON.stringify(event))
        })

        await this.consumer.run({
            eachMessage: async ({topic, partition, message}) => {

                const msg: PushEvent = JSON.parse(Buffer.from(message.value).toString());
                logger.info(`filter handler :Read %o event from Kafka broker`, msg);
                const msgPayload: { [index: string]: any } = msg.payload;
                if (!msgPayload) {
                    return;
                }

                let counter: number = 0;
                for (const filter of filters) {
                    if (msgPayload.hasOwnProperty(filter.key)) {
                        const key: string = msgPayload[filter.key];
                        switch (filter.operation) {
                            case '>':
                                if (key > filter.value) {
                                    counter++;
                                }
                                break;
                            case '<':
                                if (key < filter.value) {
                                    counter++;
                                }
                                break;
                            case '==':
                                if (key != filter.value) {
                                    counter++;
                                }
                                break;
                        }
                    }
                }
                if (eventData.operation == Operation.AND && counter != filters.length) {
                    // For AND all conditions should be met
                    return;
                }
                if (eventData.operation == Operation.OR && counter == 0) {
                    // For OR at least one condition should be met
                    return;
                }

                logger.info(`Pushing Filtered event to client %s`, Buffer.from(message.value).toString())
                socket.emit('filtered_events', JSON.stringify(message))
            },
        })
    }


    async pushToClients(mergedEvents: object[], socket: Socket, kafkaProducer: KafkaProducer, pushToTopic: string) {
        let data;
        let kafkaPayload;

        for (const mergedEvent of mergedEvents) {
            data = JSON.stringify(mergedEvent);
            logger.info(`Pushing Joined event to client %s`, JSON.stringify(mergedEvent))

            socket.emit('joined_events', data)
            kafkaPayload = {
                topic: pushToTopic,
                type: 'metric',
                payload: data
            }
            await kafkaProducer.pushEvents(JSON.stringify(kafkaPayload))
        }
    }


    async pushJoinedEventsToClient(data: string, socket: Socket, kafkaProducer: KafkaProducer) {
        logger.info(`Pushing Joined events to client %s`, this.clientName)

        const eventData: EventWithJoin = JSON.parse(data);
        let mergedData = new Map<string, object>();
        let startTime: Date = new Date();

        await this.consumer.run({
            eachMessage: async ({topic, partition, message}) => {

                const msg: PushEvent = JSON.parse(Buffer.from(message.value).toString());
                logger.info(`merged handler: Read %o event from Kafka broker`, msg);

                const msgPayload: { [index: string]: any } = msg.payload;

                const alreadyData = mergedData.get(msgPayload[eventData.key]);
                mergedData.set(msgPayload[eventData.key], {...alreadyData, ...msgPayload});

                const endTime = new Date();
                if (endTime.getSeconds() - startTime.getSeconds() > 5) {
                    logger.info(`Pushing %s merged records to client %s`, mergedData.size, this.clientName);
                    await this.pushToClients(compact([...mergedData.values()]), socket, kafkaProducer, eventData.topic);
                    startTime = endTime;
                    mergedData = new Map<string, object>();
                }
            },
        })
    }

    async pushAvgMetricEventsToClient(data: string, socket: Socket) {
        logger.info(`Pushing Joined events to client %s`, this.clientName)

        const eventData: EventWithJoin = JSON.parse(data);
        let mergedData = new Map<string, object>();
        let startTime: Date = new Date();

        await this.consumer.run({
            eachMessage: async ({topic, partition, message}) => {

                const msg: PushEvent = JSON.parse(Buffer.from(message.value).toString());
                logger.info(`merged handler: Read %o event from Kafka broker`, msg);

                const msgPayload: { [index: string]: any } = msg.payload;

                const alreadyData = mergedData.get(msgPayload[eventData.key]);
                mergedData.set(msgPayload[eventData.key], {...alreadyData, ...msgPayload});

                const endTime = new Date();
                if (endTime.getSeconds() - startTime.getSeconds() > 5) {
                    logger.info(`Pushing %s merged records to client %s`, mergedData.size, this.clientName);
                    // await this.pushToClients(compact([...mergedData.values()]), socket, eventData.topic);
                    startTime = endTime;
                    mergedData = new Map<string, object>();
                }
            },
        })
    }


}