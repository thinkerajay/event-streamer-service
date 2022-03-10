import MessageConnector from "./MessageConnector";
import {Socket} from "socket.io";
import {logger} from "../config/logger";

export let socketClientNameMap = new Map<string, Socket>();
export default class MessageHandlers {
    private readonly messageConnector: MessageConnector;

    constructor() {
        this.messageConnector = new MessageConnector();
    }

    // set clientName as key and socket as value basically as pointer maps
    setSocketClientMap(data: string, socket: Socket) {
        const eventDate: {
            clientName: string
        } = JSON.parse(data);
        socketClientNameMap.set(eventDate.clientName, socket);
    }

    async handleStartEvent(data: string) {
        logger.info(`Received start_event_push with data %o`, data)
        await this.messageConnector.initiateKafkaPush(data)
    }

    async handleStartEventWithAvgCal(data: string) {
        logger.info(`Received start_event_push_with_avg_cal with data %o`, data)
        await this.messageConnector.initiateKafkaPushWithAvgCal(data)
    }

    async handlePushEvents(data: string) {
        logger.info(`Received push_event with data %o`, data)
        await this.messageConnector.writeEvents(data)
    }

    async handlePushEventsWithCalAvg(data: string) {
        logger.info(`Received push_event_with_cal_avg with data %o`, data)
        await this.messageConnector.writeEventsWithAvgCal(data)
    }

    async handlePullEvents(data: string, socket: Socket) {
        logger.info(`Received pull_event with data %o`, data)
        await this.messageConnector.pushEventsToClient(data, socket)
    }


    async handlePullEventsWithFilter(data: string, socket: Socket) {
        logger.info(`Received pull_events_with_filter with data %o`, data)
        await this.messageConnector.pushFilteredEventsToClient(data, socket)
    }

    async handlePullEventsWithJoin(data: string, socket: Socket) {
        logger.info(`Received pull_events_with_join with data %o`, data)
        await this.messageConnector.pushJoinedEventsToClient(data, socket)
    }

    async handlePullEventsWithAvgMetric(data: string, socket: Socket) {
        logger.info(`Received pull_events_with_avg_on_metric with data %o`, data)
        await this.messageConnector.pushAvgMetricEventsToClient(data, socket)
    }

    async handleDisconnect() {
        logger.info('Received disconnect from client')
        await this.messageConnector.close()
    }


}