import 'reflect-metadata';
import {io, Socket} from 'socket.io-client';
import {Container, Inject} from "typedi";
import {EVENT_STREAMER_SERVICE_URL} from "../config/env";
import {logger} from "../config/logger";
import {Message} from "kafkajs";

class Client {
    private readonly name: string;
    private readonly topicName: string;
    private sendSocket: Socket;
    private receiveSocket: Socket;
    private readonly eventStreamerServiceURL: string;

    constructor(name: string, topicName: string, @Inject(EVENT_STREAMER_SERVICE_URL) eventStreamerServiceURL: string) {
        this.name = name;
        this.topicName = topicName;
        this.eventStreamerServiceURL = eventStreamerServiceURL;
    }

    connectAndPushEvents() {
        this.sendSocket = io(this.eventStreamerServiceURL);
        this.sendSocket.on('connect', () => {
            logger.info(`send socket is ready`, this.sendSocket.id)
        });
        logger.info('emitting start_event_push event')

        this.sendSocket.emit('start_event_push', JSON.stringify({
            topic: "ABC",
            clientName: "XYZ"
        }))

        setInterval(() => {
            logger.info('sending event ...')
            this.sendSocket.emit('push_event', JSON.stringify({
                topic: "ABC",
                payload: {
                    ip: "10.2.1.2",
                    name: this.name,
                    id: 5,
                    protocol: "tcp"
                }
            }))
        }, 5000)


    }

    connectAndPullEvents(topics: string[]) {
        this.receiveSocket = io(this.eventStreamerServiceURL);
        this.receiveSocket.on('connect', () => {
            logger.info(`Receive socket is ready`, this.receiveSocket.id)
        })

        this.receiveSocket.emit('pull_event', JSON.stringify({
            topics: ['ABC'],
            clientName: 'XYZ'
        }))
        this.receiveSocket.on('events', (data) => {
            const eventData: Message = JSON.parse(data);
            logger.info(`Received events %o`, Buffer.from(eventData.value).toString());
        })

    }
}

(async function () {
    const client = new Client('xyz', 'abc', Container.get(EVENT_STREAMER_SERVICE_URL))
    client.connectAndPushEvents()
    await new Promise((resolve) => setTimeout(resolve, 3000));
    client.connectAndPullEvents(['ABC'])

    // const kafka = new Kafka({
    //     clientId: 'event-streamer-service',
    //     brokers: ['localhost:9092'],
    //
    // })
    // const producer = kafka.producer()
    //
    // await producer.connect()
    // await producer.send({
    //     topic: 'ABC',
    //     messages: [
    //         { value: 'Hello KafkaJS user!' },
    //     ],
    // })
    //
    // const c = kafka.consumer({
    //     groupId: 'event-streamer-group'
    // })
    // await c.connect()
    // await c.subscribe({topic: 'ABC', fromBeginning:true})
    // await c.run({
    //     eachMessage: async ({topic, partition, message}) => {
    //         logger.info(`Pushing event to client %o`, message)
    //     },
    // })
})()
