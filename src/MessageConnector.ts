import 'reflect-metadata';
import DbConnector from "./DbConnector";
import KafkaConsumer from "./KafkaConsumer";
import KafkaProducer from "./KafkaProducer";
import {Container} from "typedi";
import {logger} from "../config/logger";
import {Socket} from "socket.io";





export default class MessageConnector{
    private readonly kafkaProducer: KafkaProducer;
    private readonly kafkaConsumer: KafkaConsumer;
    private readonly dbConnector: DbConnector;
    constructor() {
        this.kafkaConsumer = new KafkaConsumer();
        this.kafkaProducer = new KafkaProducer();
        this.dbConnector = Container.get(DbConnector);
    }

    async initiateKafkaPush(data: string){
        await this.kafkaProducer.createTopic(data);
    }

    async writeEvents(data: string){
        await Promise.all([await this.dbConnector.writeEvent(data), await this.kafkaProducer.pushEvents(data)]);
    }

    async pushEventsToClient(data: string, socket: Socket){
        logger.info(`received pull_event with data %o`, data)
        await this.kafkaConsumer.connectAndSubscribe(data)
        // sleep for 2secs
        await new Promise((resolve) => setTimeout(resolve, 2000));
        await this.kafkaConsumer.pushEventsToClient(socket)
    }


    async pushFilteredEventsToClient(data: string, socket: Socket){
        // subscribe to all the topics provided in the message
        await this.kafkaConsumer.connectAndSubscribe(data);
        await this.kafkaConsumer.pushFilteredEventsToClient(data, socket);
    }

    async pushJoinedEventsToClient(data: string, socket: Socket){
        // subscribe to all the topics provided in the message
        await this.kafkaConsumer.connectAndSubscribe(data);
        await this.kafkaProducer.createTopic(data);
        await this.kafkaConsumer.pushJoinedEventsToClient(data, socket, this.kafkaProducer);
    }

    async pushAvgMetricEventsToClient(data: string, socket: Socket){
        // subscribe to all the topics provided in the message
        await this.kafkaConsumer.connectAndSubscribe(data);
        await this.kafkaConsumer.pushAvgMetricEventsToClient(data, socket);
    }

    async close(){
        await this.kafkaConsumer.disconnect()
        await this.kafkaConsumer.disconnect()
        await this.dbConnector.close()
    }


}