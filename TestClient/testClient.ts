import {Container} from "typedi";
import {EVENT_STREAMER_SERVICE_URL} from "../config/env";
import EventStreamerClient from "../src/Client";

(async function () {

    const clientOne = new EventStreamerClient('sleepy-hallow', 'ABC', Container.get(EVENT_STREAMER_SERVICE_URL))
    const clientTwo = new EventStreamerClient('avengers', 'DEF', Container.get(EVENT_STREAMER_SERVICE_URL))
    const clientThree = new EventStreamerClient('batman', 'XYZ', Container.get(EVENT_STREAMER_SERVICE_URL))
    clientOne.connectAndPushEvents()
    clientTwo.connectAndPushEvents()
    clientThree.connectAndPushEvents()
    await new Promise((resolve) => setTimeout(resolve, 5000));
    //clientThree.connectAndPullEvents(['ABC', 'DEF', 'XYZ'])
    // clientThree.connectAndPullEventsWithFilter(['ABC', 'DEF', 'XYZ'], [{
    //     key: 'cpu',
    //     value: '50',
    //     operation: '>'
    // }])

    clientThree.connectAndPullEventsWithJoin(['ABC', 'DEF', 'XYZ'], 'ip', 'OMO')


})()