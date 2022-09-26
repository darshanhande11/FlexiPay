## Problem Statement

Ever thought about not attending the event further right when it’s ongoing? Ever felt like the money you paid for an event isn’t worth it? It happens! Nowadays, there are literally so many DAO events going on (mostly on discord voice channels) that sometimes people tend to register for the wrong ones which they aren’t looking for. Reasons being the “LIMITED SEATS AVAILABLE”, “The fear of missing out”, Peer Pressure etc. Also, often the event isn’t conducted in the way it has been planned and promised. At the end of it, people feel that what if there was a way which is more Flexible to register and pay for the Events without the entire money being locked as Registration Fees. Also the current NFT gated access to DAO events is not an absolute solution where user needs to buy an whole NFT as a pass for a whole bunch of events even though user is only interested in attending a few of them.

## Solution

We present FlexiPay, a Flexible Payment solution for DAO events which you attend. FlexiPay creates a stream of money flow between the Event Attendee and Organizer so that as the event starts, money flows from attendee’s wallet to organizer’s wallet for every second they attend the event. We have used Superfluid for this feature. Once the attendee stops attending the event, they won’t be charged for the remaining duration of the event.

FlexiPay comprises of two components : A Web Application and A Discord Bot. Web application allows the users to Create Events, View all the events available on FlexiPay and join the events of their choice. To record all the actions happening on the Client side, we have used Tableland which is a decentralized SQL like Relational Database. For the smooth Client side experience, we have used React.js and AntDesign Library. 

Both, the web application and bot share a common database. Discord Bot guards the event against spamming and freeloaders. The Bot runs on an algorithm which checks for unregistered users or users who do not have a Stream running, and kicks them out of the meet to prevent exploitation of Organizers and Speakers time and efforts. Bot fetches the list of the registered users from the web app’s db. We are using SuperFluid’s SDK which has a getFlow() function to check if a registered user has their stream running. We have used Node.js, Discord.js, SuperFluid and Tableland to build the bot.

## TechStack

**Web App:**

- React.js
- Ant Design (UI Library)
- Tableland (decentralized SQL like Relational Database)
- Chainlink (used as a datafeed to fetch the current price of Dai token in USD)
- IPFS & Moralis (For storing and pinning files on IPFS Distributed File System)
- SuperFluid (To establish payment stream)
- Polygon (To deploy smart contracts)

**Discord Bot:**

- Node.js
- Discord.js
- Tableland
- Superfluid

## How does it work?

### Web Application:

- Login via Metamask and click on Get Started.
- You can see the scheduled events on the Events Page. The past events have been marked as over for better UX.
- Click on any Event Card and it will redirect you to the Event Details Page.
- You can view the Event Details and can Join the event by filling the form on the same page.
- You can then join the Discord Server of the Event by clicking on the Discord Invite button.
- Just before the start of an event, you have to start the Stream by clicking on the Start Stream button on the Event Details Page.
- You can then join the discord voice channel of the Event.
- Remember to stop the Stream after you leave the Event.
- Once the Event is over, attendees can withdraw their RSVP Fees from the Event Details page of the respective Event.

### Bot:

- You can find the link to invite Bot on your discord server on the About Vlad Page on Web app.
- Select the server to which you would like to add Vlad.
- Paste the following command in your #general channel.
    
    `!init eventInfo {eventId} {vc-name}`
    
- Replace `{eventId}` with the Event Id of your event and `{vc-name}` with the Voice Channel Name on which the event would be hosted.
- Before the event starts make sure to enable bot to monitor the Voice channel by entering the command `/start-monitoring`on your #general channel.
- After the event ends, you can stop the bot from monitoring the Voice channel by entering the command `/stop-monitoring` on your #general channel.

## Use Cases

- It will be used for organizing and managing event funds and user token streams to event organizer.
- Stream gated event access (powered by discord bot) will ensure only registered + active stream attendees to attend the event.
- FlexiPay enables pay per join model where user will only be paying for the amount of time he/she is the part of the DAO events organized in discord vc.
- Time for which the stream is active could be utilized while issuing POAP tokens to the users.


## Sneak Peak
<img src="images/pic-1.png" alt="" />
- Landing page at FlexiPay
<img src="images/pic-2.png" alt="" />
- Page to create events by DAO communities with necessary details.
<img src="images/pic-3.png" alt="" />
- All Listed events are visible here.
<img src="images/pic-4-1.png" alt="" />
- Vlad info page our friendly discord bot which will help us kicking out participants with no active stream of tokens
<img src="images/pic-4-2.png" alt="" />
- Invite vlad to your server
<img width="1446" alt="image" src="https://user-images.githubusercontent.com/56120084/192184806-c6e2cb45-3083-48ac-a4ac-04f14dac4ab7.png">
- Event details page seperate for each event.
<img width="1047" alt="image" src="https://user-images.githubusercontent.com/56120084/192184615-4e52f84f-3899-4e03-8a6c-3fc5a00886fc.png">
- Section to manage streams or withdraw RSVP fees or subscribe to events.
<img src="images/pic-5.png" alt="" />
- test stream between attendee and organizer wallet 
<img src="images/pic-6.png" alt="" />
- Usage of Vlad discord bot using command.
<img width="1047" alt="image" src="https://user-images.githubusercontent.com/56120084/192184390-bea065b7-ac2b-44bd-8020-562027b40710.png">
- EPNS notifications for events

## Team:

- Nitanshu Lokhande
- Darshan Hande
