import 'reflect-metadata';
import {io, Socket} from 'socket.io-client';
import {Container, Inject} from "typedi";
import {EVENT_STREAMER_SERVICE_URL} from "../config/env";
import {logger} from "../config/logger";
import {Message} from "kafkajs";

export default class EventStreamerClient {
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
                topic: this.topicName,
                type: 'metric',
                payload: {
                    ip: `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
                    name: this.name,
                    id: 5,
                    protocol: "tcp",
                    cpu: Math.floor(Math.random() * 100),
                    memory: Math.floor(Math.random() * 100)
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
            topics,
            clientName: this.name
        }))
        this.receiveSocket.on('events', (data) => {
            const eventData: Message = JSON.parse(data);
            logger.info(`Received events %o`, Buffer.from(eventData.value).toString());
        })

    }
}

// (async function () {
//
//     const clientOne = new Client('sleepy-hallow', 'ABC', Container.get(EVENT_STREAMER_SERVICE_URL))
//     const clientTwo = new Client('avengers', 'DEF', Container.get(EVENT_STREAMER_SERVICE_URL))
//     const clientThree = new Client('batman', 'XYZ', Container.get(EVENT_STREAMER_SERVICE_URL))
//     clientOne.connectAndPushEvents()
//     clientTwo.connectAndPushEvents()
//     clientThree.connectAndPushEvents()
//     await new Promise((resolve) => setTimeout(resolve, 3000));
//     clientThree.connectAndPullEvents(['ABC', 'DEF', 'XYZ'])
//
//
// })()


