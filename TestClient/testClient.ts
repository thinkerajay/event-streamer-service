import {Container} from "typedi";
import {EVENT_STREAMER_SERVICE_URL} from "../config/env";
import EventStreamerClient from "../src/Client";

(async function () {

    //Create three clients to mimicking services sending events to server
    const clientOne = new EventStreamerClient('sleepy-hallow', 'ABC', Container.get(EVENT_STREAMER_SERVICE_URL))
    const clientTwo = new EventStreamerClient('avengers', 'DEF', Container.get(EVENT_STREAMER_SERVICE_URL))
    const clientThree = new EventStreamerClient('batman', 'XYZ', Container.get(EVENT_STREAMER_SERVICE_URL))


    clientOne.connectAndPushEvents()
    clientTwo.connectAndPushEvents()

    // un-comment this to test Bonus task simulation
    //clientThree.connectAndPushEventsWithAvgCal('push_event_with_avg_cal')
    await new Promise((resolve) => setTimeout(resolve, 5000));

    // un-comment this to test plain pull events
    //clientThree.connectAndPullEvents(['ABC', 'DEF', 'XYZ'])

    //un-comment this to test pull events with filter passed
    clientThree.connectAndPullEventsWithFilter(['ABC', 'DEF', 'XYZ'], [{
        key: 'cpu',
        value: '50',
        operation: '>'
    }])

    // un-comment this to test events merged over a key events
    //clientThree.connectAndPullEventsWithJoin(['ABC', 'DEF', 'XYZ'], 'ip', 'OMO')

    //un-comment this to test pull events with average calculated on the keys over the time period
    //clientThree.connectAndPullEventsWithAvgCal('ABC', 'XYZ', ['cpu', 'memory', 'disk']);


})()