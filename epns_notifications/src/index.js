import * as EpnsAPI from "@epnsproject/sdk-restapi";
import * as ethers from "ethers";
import dotenv from "dotenv";
import { connect } from "@tableland/sdk";
import express from "express";
import * as cron from "node-cron";
import tableNames from '../databaseConfig.js';
import fetch from 'node-fetch';
dotenv.config();
globalThis.fetch = fetch;

const app = express();
const epnsPK = `0x${process.env.EPNS_PK}`;
const signer = new ethers.Wallet(epnsPK);


const fetchEventUsers = async (eventIds, tableland) => {
    try {
        const eventUsers = new Map();
        for (let i = 0; i < eventIds.length; i++) {
            const eventUsersTL = await tableland.read(
                `SELECT username FROM ${tableNames.EVENT_USER} WHERE event_id = '${eventIds[i]}'`
            );
            const users = eventUsersTL.rows.map((eventUser) => {
                return {
                    username: eventUser[0],
                    user_meta_address: '',
                }
            });
            eventUsers.set(eventIds[i], users);
        }
        return eventUsers;
    } catch (err) {
        console.log("Error in fetching event users\n", err);
    }
}

const shouldNotify = async (startDate, startTime) => {
    const currobj = new Date();
    const dateArr = startDate.split("/");
    const timeArr = startTime.split(":");
    const enddobj = new Date(
        dateArr[2],
        dateArr[1] - 1,
        dateArr[0],
        timeArr[0],
        timeArr[1]
    );
    if (enddobj.getMilliseconds() - currobj.getMilliseconds() < 1800000 && enddobj.getMilliseconds() - currobj.getMilliseconds() > 0) {
        return true;
    }
    return false;
}

const initTableLand = async () => {
    try {
        const tableLandWallet = new ethers.Wallet(process.env.TABLELAND_PK);
        const provider = new ethers.providers.AlchemyProvider(
            "maticmum",
            process.env.QUICKNODE_API_KEY
        );
        const signer = tableLandWallet.connect(provider);
        const tableland = await connect({
            signer,
            network: "testnet",
            chain: "polygon-mumbai",
        });

        const eventsResp = await tableland.read(
            `SELECT * FROM ${tableNames.EVENT_DETAILS}`
        );

        let events = new Map();

        for (let i = 0; i < eventsResp.rows.length; i++) {
            const event = eventsResp.rows[i];
            const notify = await shouldNotify(event[4], event[6]);
            if (notify) 
                events.set(event[0], event);
        }
        let eventIdsJS = [];
        console.log(events.size);
        if (events.size > 0)
            eventIdsJS = Array.from(events.keys());
        // console.log(eventIdsJS);
        let eventUsers = new Map();
        if (eventIdsJS.length > 0)
            eventUsers = await fetchEventUsers(eventIdsJS, tableland);
        // console.log(eventUsers);
        // const eventUsers = await fetchEventUsers(eventIdsJS, tableland);

        let eu = new Map();
        if(eventUsers.size > 0) {
            for(const eventId of eventUsers.keys()) {
                let users = eventUsers.get(eventId);
                let u = [];
                for(let i = 0 ; i < users.length; i++) {
                    const user = users[i];
                    const userMetaAddress = await tableland.read(
                        `SELECT user_meta_address, username FROM ${tableNames.USER_META_ADDRESS} WHERE username = '${user.username}'`
                    )
                    u.push({ username: user.username, user_meta_address: userMetaAddress.rows.length > 0 ? userMetaAddress.rows[0][0] : '' });
                }
                eu.set(eventId, u);
            }
        }

        eventUsers = eu;
        // console.log(eventUsers);
        // console.log(events.get('rcZlbiH'));

        if(eventUsers.size > 0) {
            for (const eventId of eventUsers.keys()) {
                const event = events.get(eventId);
                const users = eventUsers.get(eventId);
                for(let i=0; i < users.length ; i++) {
                    const user = users[i];
                    if (user.user_meta_address !== '') {
                        const body = `${event[1]} is starting soon. Join now!`;
                        await sendNotification(
                            user.user_meta_address, body,
                            "https://flexi-pay.netlify.app/",
                            `${process.env.MORALIS_IPFS_URL}${event[2]}`,
                        );
                    }
                }
            }
        }

    } catch (err) {
        console.log("Error in connecting to TableLand", err);
    }
};


const sendNotification = async (recipientAddress, body, cta, img) => {
    try {
        const apiResponse = await EpnsAPI.payloads.sendNotification({
            signer,
            type: 3, // target
            identityType: 2, // direct payload
            notification: {
                title: `[SDK-TEST] notification TITLE:`,
                body: `[sdk-test] notification BODY`
            },
            payload: {
                title: 'Event starting soon...',
                body: body,
                cta: cta,
                img: img,
            },
            recipients: `eip155:42:${recipientAddress}`, // recipient address
            channel: 'eip155:42:0xBE2d52e161553772D57801E0Dd0A321b3e8bE534', // your channel address
            env: 'staging'
        });

        // apiResponse?.status === 204, if sent successfully!
        console.log('API repsonse: ', apiResponse.status);
    } catch (err) {
        console.error('Error: ', err);
    }
}

cron.schedule("*/30 * * * * ", async () => {
    initTableLand();
    console.log("Calling initTableLand every 30 minutes");
});

app.listen(process.env.PORT || 8000, () => {
    console.log("Server listening on port 8000...")
})
