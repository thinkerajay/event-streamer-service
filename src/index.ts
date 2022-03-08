import express from 'express';
import * as http from 'http';
import {Server} from 'socket.io';
import KafkaProducer from "./KafkaProducer";
import KafkaConsumer from "./KafkaConsumer";
import {logger} from "../config/logger";

const app = express();
const eventStreamerService = http.createServer(app);
const io = new Server(eventStreamerService);

const port: number = Number(process.env.PORT) || 8547;


io.on('connection',  (socket) => {
    logger.info('server received connection', socket.id);

    const producer = new KafkaProducer();
    const consumer = new KafkaConsumer();
    socket.on('start_event_push', async (data: any) => {
        logger.info(`received start_event_push with data %o`, data)
        await producer.createTopic(data);
    })
    socket.on('push_event', async (data: any) => {
        logger.info(`received push_event with data %o`, data)
        await producer.pushEvents(data);
    })

    socket.on('pull_event', async (data: any) => {
        logger.info(`received pull_event with data %o`, data)
        await consumer.connectAndSubscribe(data)
        await new Promise((resolve) => setTimeout(resolve, 2000));
        await consumer.pushEventsToClient(socket)

    })

    socket.on('disconnect', async (reason) => {
        logger.info('received disconnect from client')
        await producer.disconnect()
        await consumer.disconnect()
    })

});


eventStreamerService.listen(port, () => {
    logger.info('listening on *:8547 server');
});