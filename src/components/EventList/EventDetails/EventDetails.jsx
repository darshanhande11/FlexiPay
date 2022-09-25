import React, { useState, createRef } from "react";
import "./EventDetails.css";
import { Form, Input, Button, Steps, Alert, message } from "antd";
import { Wallet, providers } from "ethers";
import { connect } from "@tableland/sdk";
import { useEffect } from "react";
import Loader from "../../../shared/Loader/Loader";
import GetAccount from "../../../hooks/GetAccount";
import { Framework } from "@superfluid-finance/sdk-core";
import { ethers } from "ethers";
import tableNames from "../../../databaseConfig";
import FlexiPayArtifact from "../../../Ethereum/FlexiPay.json";
import GetContract from "../../../hooks/GetContract";
import addresses from "../../../config";
import ERC20ABi from "../../../Ethereum/ERC20ABI.json";
import ProofOfAttendenceAbi from "../../../Ethereum/ProofOfAttendence.json";
import { useMoralis, useMoralisFile } from "react-moralis";
import getDAIToUsdPrice from "../../../services/DaiToUsdPrice";
import * as EpnsAPI from "@epnsproject/sdk-restapi";

//! we can also include a feature to notify user to stop the stream once the event got over

const { Step } = Steps;
const EventDetails = () => {
  const { authenticate, isAuthenticated, user } = useMoralis();
  const { error, isUploading, moralisFile, saveFile } = useMoralisFile();
  let ethProvider = new ethers.providers.Web3Provider(window.ethereum);
  let flexiPayContract = new ethers.Contract(addresses.FlexiPay, FlexiPayArtifact.abi, ethProvider.getSigner(0));
  let ProofOfAttendenceContract = GetContract(
    addresses.ProofOfAttendence,
    ProofOfAttendenceAbi.abi
  );
  let superFakeDAITokenContract = GetContract(
    addresses.SuperFakeDAIToken,
    ERC20ABi.abi
  );
  let userAddress = GetAccount();
  const [event, setEvent] = useState(null);
  const [userInfo, setUserInfo] = useState({
    name: "",
    username: "",
  });
  const [isLoading, setIsLoading] = useState(false);
  const event_id = window.location.href.split("/")[5];
  console.log(event_id);
  const DAI_TO_WEI = 1.689 * 10 ** 16; // for DAI per min
  const discordInvite = "https://discord.gg/invite/invite";
  const [form] = Form.useForm();
  const [isStreamStarted, setIsStreamStarted] = useState(false);
  const [isStreamEnded, setIsStreamEnded] = useState(false);
  const [tableLandState, setTableLandState] = useState(null);
  const [DaiToUsdPrice, setDaiToUsdPrice] = useState(1);
  const [currentUsername, setCurrentUsername] = useState(null);
  const [superFluidSignerState, setSuperFluidSignerState] = useState(null);
  const [superFluidInstance, setSuperfluidInstance] = useState(null);
  const [isUserRegisteredForEvent, setIsUserRegisteredForEvent] =
    useState(false);
  const [isUserWithDrawRsvp, setIsUserWithdrawnRsvp] = useState(false);
  const DAI_FLOW_RATE = 385802469135;
  const [isUserSub, setIsUserSub] = useState(null);
  

  const reArrangeEventsData = (eventData) => {
    // eventData is an array we just need to fix it
    let tempEventData = [];
    tempEventData[0] = eventData[4];
    tempEventData[1] = eventData[6];
    tempEventData[2] = eventData[7];
    tempEventData[3] = eventData[0];
    tempEventData[4] = eventData[14];
    tempEventData[5] = eventData[2];
    tempEventData[6] = eventData[15];
    tempEventData[7] = eventData[3];
    tempEventData[8] = eventData[5];
    tempEventData[9] = eventData[1];
    tempEventData[10] = eventData[13];
    tempEventData[11] = eventData[12];
    tempEventData[12] = eventData[8];
    tempEventData[13] = eventData[9];
    tempEventData[14] = eventData[10];
    tempEventData[15] = eventData[11];
    return tempEventData;
  };

  const alchemyToEventNode = (events) => {
    let newEvents = {
      columns: [],
      rows: [],
    };

    for (var prop in events) {
      let newEvent = [];
      if (Object.prototype.hasOwnProperty.call(events, prop)) {
        // let sortedEvent = sortObjectByKeys(events[prop]);
        if (newEvents.columns.length === 0) {
          newEvents.columns = Object.keys(events[prop]);
        }
        newEvent = Object.values(events[prop]);
        if (newEvent.length === 16) newEvent = reArrangeEventsData(newEvent);
        newEvents.rows.push(newEvent);
      }
    }
    return newEvents;
  };

  const initTableLand = async () => {
    try {
      setIsLoading(true);
      let price = await getDAIToUsdPrice();
      setDaiToUsdPrice(price);
      const wallet = new Wallet(process.env.REACT_APP_PRIVATE_KEY);
      const provider = new providers.AlchemyProvider(
        "maticmum",
        process.env.REACT_APP_ALCHEMY_API_KEY
      );
      const signer = wallet.connect(provider);
      const tableland = await connect({
        signer,
        network: "testnet",
        chain: "polygon-mumbai",
      });
      setTableLandState(tableland);
      const eventFetch = await tableland.read(
        `SELECT * FROM ${tableNames.EVENT_DETAILS} WHERE event_id = '${event_id}'`
      );
      // console.log(event.rows[0]);
      let currentUser = await tableland.read(
        `SELECT username FROM ${tableNames.USER_META_ADDRESS} WHERE user_meta_address = '${userAddress}'`
      );
      let data = await tableland.read(
        `select * from ${tableNames.USER_META_ADDRESS}`
      )
      console.log(" this is full table for user meta address ", data);
      console.log(" this is userAddress ", userAddress);
      const event = alchemyToEventNode(eventFetch);
      console.log(" this is current user fetch ", currentUser);
      setEvent(event.rows[0]);
      console.log(" updated events ", event);
      currentUser = alchemyToEventNode(currentUser);
      console.log(" current user updated ", currentUser);
      setCurrentUsername(currentUser.rows);
      if (currentUser.rows.length > 0) {
        const userName = currentUser.rows[0][0];
        console.log(" this is username ", userName);
        console.log(" this is username fetched ", userName);
        let userRegisteredEvents = await tableland.read(
          `SELECT event_id FROM ${tableNames.EVENT_USER} WHERE username = '${userName}'`
        );
        console.log(
          "Events for which current user is registered ",
          userRegisteredEvents
        );
        userRegisteredEvents = alchemyToEventNode(userRegisteredEvents);
        const userEvents = userRegisteredEvents.rows.map((events) => events[0]);
        if (userEvents.includes(event_id)) {
          setIsUserRegisteredForEvent(true);

          const url = process.env.REACT_APP_ALCHEMY_URL;
          const customHttpProvider = new ethers.providers.JsonRpcProvider(url);

          let sf = await Framework.create({
            chainId: 80001,
            provider: customHttpProvider,
          });
          console.log(" this is sf ", sf);
          setSuperfluidInstance(sf);
          const superFluidProvider = new ethers.providers.Web3Provider(
            window.ethereum
          );
          const superFluidSigner = superFluidProvider.getSigner();
          setSuperFluidSignerState(superFluidSigner);

          let checkStream = await sf.cfaV1.getFlow({
            superToken: addresses.SuperFakeDAIToken,
            sender: userAddress,
            receiver: event.rows[0][10],
            providerOrSigner: signer,
          });

          if (checkStream.flowRate > 0) {
            setIsStreamStarted(true);
            setIsStreamEnded(false);
          } else {
            setIsStreamEnded(true);
            setIsStreamStarted(false);
          }
          console.log(" checking flow rate ", checkStream);
        }
      }

      let isUserWithDrawRsvp = await checkUserWithdrawnRsvpFee();
      setIsUserWithdrawnRsvp(isUserWithDrawRsvp);
      setIsLoading(false);
    } catch (err) {
      message.warning("Some error occured during initialization");
      console.log(err);
    }
  };

  const checkUserWithdrawnRsvpFee = async () => {
    try {
      if(flexiPayContract) {
        let isUserWithDrawnRsvp = await flexiPayContract.isRsvpFeeWithDrawn(
          event_id
        );
        return isUserWithDrawnRsvp;
      }
    } catch (err) {
      console.log(err);
    }
  }

  useEffect(() => {
    window.Buffer = Buffer;
    initTableLand();
    isUserSubscribed();
  }, []);

  const isUserSubscribed = async () => {
    try {
      const subs = await fetchUserSubs();
      const fpChannel = process.env.REACT_APP_EPNS_CHANNEL;
      for(let i=0;i<subs.length;i++) {
        if(subs[i].channel === fpChannel) {
          console.log("user is subscribed");
          setIsUserSub(false);
          return ;
        }
      }
      initTableLand();
      setIsUserSub(false);
    } catch (err) {
      console.log(err);
    }
  }

  const subscribeToChannel = async () => {
    const epnsWallet = new Wallet(`0x${process.env.REACT_APP_EPNS_PK}`);
    const provider = new providers.AlchemyProvider(
      "maticmum",
      process.env.REACT_APP_ALCHEMY_API_KEY
    );
    const epnsSigner = epnsWallet.connect(provider);
    try {
      const apiResp = await EpnsAPI.channels.subscribe({
        signer: epnsSigner,
        channelAddress: `eip155:42:${process.env.REACT_APP_EPNS_CHANNEL}`, // channel address in CAIP
        userAddress: `eip155:42:${userAddress}`, // user address in CAIP
        onSuccess: () => {
         console.log('opt in success');
        },
        onError: () => {
          console.error('opt in error');
        },
        env: 'staging'
      })
      setIsUserSub(false);
      message.success("Subscribed to Event Notifications");
    } catch (e) {
      message.error("Some error occured while subscribing");
      console.error(e);
    }
  }

  const unsubscribeFromChannel = async () => {
    const epnsWallet = new Wallet(`0x${process.env.REACT_APP_EPNS_PK}`);
    const provider = new providers.AlchemyProvider(
      "maticmum",
      process.env.REACT_APP_ALCHEMY_API_KEY
    );
    const epnsSigner = epnsWallet.connect(provider);
    try {
      const apiResp = await EpnsAPI.channels.unsubscribe({
        signer: epnsSigner,
        channelAddress: `eip155:42:${process.env.REACT_APP_EPNS_CHANNEL}`, // channel address in CAIP
        userAddress: `eip155:42:${userAddress}`, // user address in CAIP
        onSuccess: () => {
         console.log('opt out success');
        },
        onError: () => {
          console.error('opt out error');
        },
        env: 'staging'
      })
      console.log(apiResp.status);
      setIsUserSub(false);
      message.success("Unsubscribed from Event Notifications");
    } catch (e) {
      message.error("Some error occured while unsubscribing");
      console.error(e);
    }
  }

  const fetchUserSubs = async () => {
    const subscriptions = await EpnsAPI.user.getSubscriptions({
      user: `eip155:42:${userAddress}`, // user address in CAIP
      env: 'staging'
    });
    return subscriptions;
  }

  const isUserExist = async (username) => {
    try {
      // username = "Nitanshu Lokhande"
      let FIND_USER_NAME = `SELECT username from ${tableNames.USER_META_ADDRESS} where username = '${username}'`;
      let isUserNameExist = await tableLandState.read(FIND_USER_NAME);
      isUserNameExist = alchemyToEventNode(isUserNameExist);
      return isUserNameExist.rows.length > 0;
    } catch (err) {
      console.log(err);
    }
  };

  const registerForEvent = async () => {
    try {
      const { name, username } = userInfo;
      if (name && username) {
        setIsLoading(true);
        const EVENTID_USER_QUERY = `INSERT INTO ${tableNames.EVENT_USER} (event_id, username, name) VALUES ('${event_id}', '${username}', '${name}')`;
        const USER_USER_META_QUERY = `INSERT INTO ${tableNames.USER_META_ADDRESS} (username, user_meta_address) VALUES ('${username}', '${userAddress}')`;
        const insertResp = await tableLandState.write(EVENTID_USER_QUERY);
        console.log("Insert into event user table resp ", insertResp);
        const isUserInTable = await isUserExist(username);
        console.log("this is user exist ", isUserInTable);
        if (!isUserInTable) {
          console.log(" there is not username already ");
          // first check if there is already an entry for user in user meta address table if yes then skip below write op
          const insertRespMeta = await tableLandState.write(
            USER_USER_META_QUERY
          );
          console.log("insert into user meta table response ", insertRespMeta);
        }

        let eventRsvpTxn = await superFakeDAITokenContract.transfer(
          addresses.FlexiPay,
          ethers.utils.parseEther(String(event[13]))
        );
        await eventRsvpTxn.wait();
        let eventRespUpdateTxn = await flexiPayContract.registerForEvent(
          event_id,
          event[13],
          { gasLimit: "9000000" }
        );
        await eventRespUpdateTxn.wait();

        //! handles rest corner cases
        message.success("Registered for event successfully");
      } else {
        message.warning("Please fill all the fields");
      }
      setIsLoading(false);
      initTableLand();
    } catch (err) {
      setIsLoading(false);
      message.warning("Some error occured during registration");
      console.log(err);
    }
  };

  const isEventOver = (endDate, endTime) => {
    const currdobj = new Date();
    const dateArr = endDate.split("/");
    const timeArr = endTime.split(":");
    const enddobj = new Date(
      dateArr[2],
      dateArr[1] - 1,
      dateArr[0],
      timeArr[0],
      timeArr[1]
    );
    if (currdobj > enddobj) {
      console.log("event is over");
      return true;
    }
    return false;
  };

  const onChangeHandler = (e) => {
    setUserInfo({ ...userInfo, [e.target.id]: e.target.value });
    console.log("updated user data ", userInfo);
  };

  //! Assumption is that per hr rate is given
  const calculateFlowRate = () => {
    // per hr rate of event -> event[12]
    let hourlyEventRate = event[12];
    let monthlyEventRate = 720 * hourlyEventRate;
    let flowRate = monthlyEventRate * DAI_FLOW_RATE;
    return flowRate;
  };

  const startStream = async () => {
    try {
      console.log(
        " this is shitty receiver ",
        ethers.utils.formatEther(ethers.BigNumber.from(event[12]).toString()) *
          3600 *
          24 *
          30
      );
      let calculatedFlowRate = calculateFlowRate();
      console.log(" this is rate ", calculatedFlowRate);
      const createFlowOperation = superFluidInstance.cfaV1.createFlow({
        flowRate: calculatedFlowRate, // event flow rate
        receiver: event[10], // event org meta address
        superToken: addresses.SuperFakeDAIToken,
      });
      // // console.log(" this is diacontract address ")
      await createFlowOperation.exec(superFluidSignerState);
      console.log(" create flow operation response ", createFlowOperation);
      setIsStreamEnded(false);
      setIsStreamStarted(true);
      message.success("Stream started successfully");
    } catch (err) {
      message.warning("Some error occured during stream start");
      console.log(err);
    }
  };

  const stopStream = async () => {
    try {
      console.log(
        userAddress + " " + event[10] + " " + addresses.SuperFakeDAIToken
      );
      const deleteFlowOperation = superFluidInstance.cfaV1.deleteFlow({
        sender: userAddress,
        receiver: event[10],
        superToken: addresses.SuperFakeDAIToken,
      });
      await deleteFlowOperation.exec(superFluidSignerState);
      console.log(" delete flow operation response ", deleteFlowOperation);
      setIsStreamStarted(false);
      setIsStreamEnded(true);
      message.success("Stream ended successfully");
    } catch (err) {
      message.warning("Some error occured during stopping stream");
      console.log(err);
    }
  };

  const withDrawRsvpFee = async () => {
    try {
      setIsLoading(true);
      let withDrawTxn = await flexiPayContract.withDrawRsvpFee(event_id, { gasLimit: 9000000 });
      await withDrawTxn.wait();
      setIsLoading(false);
      initTableLand();
      message.success("Withdraw RSVP fee successfully completed!");
    } catch (err) {
      setIsLoading(false);
      message.warning("Some error occured while withdraw!");
      console.log(err);
    }
  };

  const claimEventNFT = async () => {
    try {
      setIsLoading(true);
      let nftMetaData = {
        name: event[1],
        image: `${process.env.REACT_APP_MORALIS_IPFS_URL}${event[2]}`,
        description: event[3],
      };
      if (!isAuthenticated) {
        authenticate();
      }
      let fileResponse = await saveFile(
        "temp.json",
        { base64: btoa(JSON.stringify(nftMetaData)) },
        { saveIPFS: true }
      );
      console.log(fileResponse.ipfs());
      let nftMintTxn = await ProofOfAttendenceContract.safeMint(
        userAddress,
        `${process.env.REACT_APP_MORALIS_IPFS_URL}${fileResponse._hash}`
      );
      await nftMintTxn.wait();
      setIsLoading(false);
    } catch (err) {
      console.log(err);
    }
  };
  // 0 - event_id
  // 1 - event_name
  // 2 - event_poster
  // 3 - about_event
  // 4 - start_date
  // 5 - end_date
  // 6 - start_time
  // 7 - end_time
  // 8 - event_link
  // 9 - discord_vc
  // 10 - org_meta_addr
  // 11 - org_discord_username
  // 12 - event_rate
  // 13 - event_rsvp_fee
  // 14 - is_nft
  // 15 - nft_hash

  const getDAIToUsdPriceFromDataFeed = async (price) => {
    try {
      let daiToUsdPrice = await getDAIToUsdPrice();
      // return price * daiToUsdPrice;
      console.log(daiToUsdPrice);
      return 10;
    } catch (err) {
      console.log(err);
    }
  };

  return (
    <>
      {event && !isLoading ? (
        <div className="ed-div">
          <h1 className="ed-heading">Event Details</h1>
          {/* <button onClick={() => createTables()}> Create tables </button> */}
          <img
            src={`${process.env.REACT_APP_MORALIS_IPFS_URL}${event[2]}`}
            alt={"Event Cover Img"}
            className="ed-img"
          />
          {/* <button onClick={() => isUserExist()} > check user </button> */}
          <div className="ed-text-div">
            <div className="ed-card">
              <h2 className="ed-card-title">{event[1]}</h2>
              <div className="ed-card-text">{event[3]}</div>
              <div className="ed-card-text">Event ID: {event[0]}</div>
              <div className="ed-card-text">Start Date: {event[4]}</div>
              <div className="ed-card-text">End Date: {event[5]}</div>
              <div className="ed-card-text">Start Time: {event[6]}</div>
              <div className="ed-card-text">End Time: {event[7]}</div>
              <div className="ed-card-text">Discord VC Name: {event[9]}</div>
              <div className="ed-card-text">
                Org MetaMask Address: {event[10]}
              </div>
              <div className="ed-card-text">
                Event Rate: {event[12] * DaiToUsdPrice} Dai / hr
              </div>
              <div className="ed-card-text">
                Event RSVP Fee: {event[13] * DaiToUsdPrice} Dai 
              </div>
            </div>
          </div>
          {/* && !isEventOver(event[5], event[7]) */}
          {!isUserRegisteredForEvent ? (
            <div className="ed-join-event-div">
              <h1 className="ed-heading">Join Event</h1>
              <Alert
                message="Your current signed in wallet account would be used to register for the event"
                className="ed-alert"
                type="info"
                showIcon
              />
              <div className="ed-join-event-slider-div">
                <Form form={form} className="ed-join-form" layout="vertical">
                  <Form.Item
                    className="ed-form-item"
                    label="Full Name"
                    name="fullName"
                    rules={[
                      {
                        required: true,
                        message: "Please input your Full Name!",
                      },
                    ]}
                  >
                    <Input
                      placeholder="Full Name"
                      name="Full Name"
                      id="name"
                      onChange={(e) => onChangeHandler(e)}
                    />
                  </Form.Item>
                  <Form.Item
                    className="ed-form-item"
                    label="Discord Username"
                    name="discordUsername"
                    rules={[
                      {
                        required: true,
                        message: "Please input your discord username!",
                      },
                    ]}
                  >
                    <Input
                      placeholder="Discord Username"
                      name="Discord Username"
                      id="username"
                      onChange={(e) => onChangeHandler(e)}
                    />
                  </Form.Item>
                  <Form.Item className="ed-submit-form-item">
                    <Button
                      className="ed-submit-btn"
                      type="primary"
                      htmlType="submit"
                      onClick={() => {
                        registerForEvent();
                      }}
                    >
                      Register
                    </Button>
                  </Form.Item>
                </Form>
              </div>
            </div>
          ) : null}

          {isUserRegisteredForEvent ? (
            <div className="ed-after-joining-div">
              <Alert
                message="You are registered for the Event !"
                type="success"
                showIcon
                className="ed-alert"
              />
              <Alert
                message="Please join the following Discord Server to participate in the event !"
                type="info"
                showIcon
                className="ed-alert"
              />
              <div
                className="ed-di-div"
                onClick={() => window.open(`${event[8]}`, "_blank")}
              >
                <img
                  src="/assets/images/discord-logo.png"
                  alt="discord-logo.png"
                  className="ed-discord-img"
                />
                <span className="ed-di-text">Discord Invite</span>
              </div>
              {
                (isUserSub === null) ?
                <Loader /> 
                :
                (!isUserSub) ?
                <div>
                  <Alert
                    message="Subscribe to our EPNS Channel if you want to be notified when the event is about to start !"
                    type="info"
                    showIcon
                    className="ed-alert"
                  />
                  <div
                    className="ed-subs-div"
                    onClick={() => subscribeToChannel()}
                  >
                    <img
                      src="/assets/images/epns-logo.jpg"
                      alt="epns-logo.jpg"
                      className="ed-epns-img"
                    />
                    <span className="ed-di-text">Subscribe to Notifications</span>
                  </div>
                </div>
                :
                <div>
                  <Alert
                    message="If you unsubscribe from receiving Event notifications, you will not be receive a reminder notification for the event !"
                    type="warning"
                    showIcon
                    className="ed-alert"
                  />
                  <div
                    className="ed-unsubs-div"
                    // onClick={() => unsubscribeFromChannel()}
                  >
                    <img
                      src="/assets/images/epns-logo.jpg"
                      alt="epns-logo.jpg"
                      className="ed-epns-img"
                    />
                    <span className="ed-di-text">You are Subscribed to Notifications</span>
                  </div>
                </div>
              }
            </div>
          ) : null}

          {isUserRegisteredForEvent && !isStreamStarted ? (
            <div className="ed-stream-div">
              <h1 className="ed-stream-heading">Stream Settings</h1>
              <Alert
                message="Please start the stream just before joining the Discord Voice channel for the Event."
                type="warning"
                showIcon
                className="ed-alert"
              />
              <Button
                className="ed-stream-btn"
                type="danger"
                onClick={() => startStream()}
              >
                Start Stream
              </Button>
            </div>
          ) : isUserRegisteredForEvent && !isStreamEnded ? (
            <div className="ed-stream-div">
              <h1 className="ed-stream-heading">Stream Settings</h1>
              <Alert
                message="Please end the stream only when you do not want attend event any further or when the event ends."
                type="warning"
                showIcon
                className="ed-alert"
              />
              <Button
                className="ed-stream-btn"
                type="danger"
                onClick={() => stopStream(true)}
              >
                End Stream
              </Button>
            </div>
          ) : null}
          {isUserRegisteredForEvent && !isUserWithDrawRsvp && isEventOver(event[5], event[7]) ?  (
            <>
              <div className="ed-wf-div">
                <h1 className="ed-heading">Withdraw RSVP Fees</h1>
                <Alert
                  message="The RSVP fees would be transferred to the MetaMask account you are currently signed in with."
                  className="ed-alert"
                  type="info"
                  showIcon
                />
                <div className="ed-di-div" onClick={() => withDrawRsvpFee()}>
                  <span className="ed-di-text">Withdraw RSVP Fees</span>
                </div>
              </div>
              {/* <div className="ed-wf-div">
          <h1 className="ed-heading">Claim your event Attendence NFT</h1>
          <div
              className="ed-di-div"
              onClick={() => claimEventNFT()}
            >
              <span className="ed-di-text">Claim</span>
            </div>
        </div> */}
            </>
          ) : null}
        </div>
      ) : (
        <Loader />
      )}
    </>
  );
};

export default EventDetails;
