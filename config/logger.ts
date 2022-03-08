import winston from 'winston';


export const logger = winston.createLogger({
    level: 'info',
    defaultMeta: {
        application: 'EventStreamerService',
        env: 'production',
    },
    format: winston.format.combine(winston.format.splat(), winston.format.timestamp(), winston.format.errors({ stack: true }), winston.format.json()),
    transports: [
        new winston.transports.Console({
            format: winston.format.json(),
            stderrLevels: ['error'],
        }),
    ],
});
