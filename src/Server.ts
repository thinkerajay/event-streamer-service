import express from 'express';
import * as http from 'http';
import {Server} from 'socket.io';
import {logger} from "../config/logger";
import MessageHandlers from "./MessageHandlers";

const app = express();
const eventStreamerService = http.createServer(app);
const io = new Server(eventStreamerService);

const port: number = Number(process.env.PORT) || 8547;


io.on('connection', (socket) => {
    logger.info('EventStreamerServer received connection', socket.id);

    const messageHandlers: MessageHandlers = new MessageHandlers()

     // Accept a stream of events from a client to a topic.
    socket.on('start_event_push', async (data: string) => {
        await messageHandlers.handleStartEvent(data);
    })
    socket.on('push_event', async (data: string) => {
        await messageHandlers.handlePushEvents(data)
    })


    // A client should be able to ask the server for a stream of current events from a list of
    // topics.
    socket.on('pull_event', async (data: string) => {
        await messageHandlers.handlePullEvents(data, socket)
    })

    // The client can also ask the server to filter the stream of events before serving the
    // events.
    socket.on('pull_events_with_filter', async (data: string)=>{
        await messageHandlers.handlePullEventsWithFilter(data, socket)
    })

    // The server should be able to join/combine events from two different topics and push
    // to another topic.
    socket.on('pull_events_with_join', async (data: string)=>{
        await messageHandlers.handlePullEventsWithJoin(data, socket)
    })

    socket.on('pull_events_with_avg_on_metric', async (data: string)=>{
        await messageHandlers.handlePullEventsWithAvgMetric(data, socket)
    })

    socket.on('disconnect', async (reason) => {
        logger.info(`%s disconnected with reason %s`, socket.id, reason)
        await messageHandlers.handleDisconnect()
    })

});


eventStreamerService.listen(port, () => {
    logger.info('listening on *:8547 server');
});