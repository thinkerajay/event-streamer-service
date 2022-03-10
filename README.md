# event-streamer-service

## Pre-requisites
- Homebrew
- Zookeeper
- Kafka
- Mongod
- MongoDb Compass
- node 17.6.0

## How to start event streamer service?
- brew services start zookeeper
- brew services start kafka
- mongod --dbpath ~/data/db 
- cd <project-path>
- in one terminal run `npm run start`

## How to simulate different aspects of the assignment?
#### uncomment method calls in simulatorClient.ts to simulate different points of the assignment
- in other terminal run `ts-node  ts-node simulator/simulatorClient.ts` 


## How to verify data?
- Open Mongodb compass
  - select events-streamer-db
  - select events collection
- View terminal for different events received by testClient
  
