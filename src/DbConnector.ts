import 'reflect-metadata';

import {MongoClient} from 'mongodb';
import {Inject, Service} from "typedi";
import {MONGO_DB_URI} from "../config/env";
import {logger} from "../config/logger";
import {PushEvent} from "./KafkaProducer";

@Service()
export default class DbConnector {
    private client: MongoClient;

    constructor(@Inject(MONGO_DB_URI) mongoDbUri: string) {
        this.client = new MongoClient(mongoDbUri)
    }

    async writeEvent(data: string) {
        try {
            await this.client.connect();
            const eventData: PushEvent = JSON.parse(data)
            const payload = {
                topic: eventData.topic,
                type: eventData.type,
                createdAt: Math.floor((new Date()).getTime() / 1000),
                ...eventData.payload
            }

            const database = this.client.db("events-streamer-db");
            const eventsCollection = database.collection("events");

            const result = await eventsCollection.insertOne(payload);
            logger.warn(`A document was inserted with the _id: ${result.insertedId}`);
        } catch (e) {
            logger.warn('MongoDB write exception %o', e)
            await this.client.close();
        }
    }

    async close(){
        await this.client.close()
    }


}

