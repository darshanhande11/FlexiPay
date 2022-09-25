import Axios from 'axios';
import addresses from '../config'

const getDaiToUsdPrice = async () => {
    const daiPriceUrl = `https://api.covalenthq.com/v1/pricing/historical_by_addresses_v2/137/usd/${addresses.DaiContract}/?&key=${process.env.REACT_APP_COVALENT_API}`
    try {
        const res = await Axios.get(
            daiPriceUrl,
            {
                headers: {
                    Accept: "application/json",
                }
            }
        );
        console.log(" this is response ", res.data.data[0].prices[0].price);
        return res.data.data[0].prices[0].price;
    } catch (err) {
        console.log(err);
    }
}

export default getDaiToUsdPrice;
