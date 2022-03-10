import {Container, Token} from "typedi";

export const EVENT_STREAMER_SERVICE_URL = new Token<string>('EVENT_STREAMER_SERVICE_URL');
export const MONGO_DB_URI = new Token<string>('MONGO_DB_URI');

Container.set(EVENT_STREAMER_SERVICE_URL, process.env.EVENT_STREAMER_SERVICE_URL || 'ws://localhost:8547');
Container.set(MONGO_DB_URI, process.env.MONGO_DB_URI || 'mongodb://127.0.0.1:27017');