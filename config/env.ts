import {Container, Token} from "typedi";

export const EVENT_STREAMER_SERVICE_URL = new Token<string>('BEESWAX_ENDPOINT');

Container.set(EVENT_STREAMER_SERVICE_URL, process.env.EVENT_STREAMER_SERVICE_URL || 'ws://localhost:8547');