
const csv = require('csv')
const fs = require('fs');
const { promisify } = require("util");
const fetch = require('node-fetch');
const stats = require("stats-lite")
const wcc = require('world-countries-capitals');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
var NodeGeocoder = require('node-geocoder');
const redis = require("redis");
const cache = redis.createClient();
const getCacheAsync = promisify(cache.get).bind(cache);
const setCacheAsync = promisify(cache.set).bind(cache);
const fahrenheitToCelsius = require('fahrenheit-to-celsius');


const overrideCountryCity = Object.freeze({
    China: 'Wuhan',
    Philippines: 'Manila',
    "Czech Republic": "Prague"
})

const getValue = async (key) => {
    const data = await getCacheAsync(key);
    if (!data) {
        return null;
    }
    return JSON.parse(data);
}

const setValue = async (key, value) => {
    return await setCacheAsync(key, JSON.stringify(value));
}

const fetchWithCache = async (uri) => {
    try {
        let data = await getValue(uri);
        if (!data || data.error) {
            console.log("fetching: ", uri);
            data = await fetch(uri);
            data = await data.json();
            if (data.error) {
                throw new Error(data.error);
            }
            await setValue(uri, data);
        }
        return data;
    } catch (error) {
        console.log({ error });
        return null;
    }
}

var geocoderOptions = {
    provider: 'google',

    // Optional depending on the providers
    httpAdapter: 'https', // Default
    apiKey: process.env.API_KEY, // for Mapquest, OpenCage, Google Premier
    formatter: null         // 'gpx', 'string', ...
};

var geocoder = NodeGeocoder(geocoderOptions);


const calculateMedianGrowthRate = (country, data) => {
    const countryData = data.filter(row => row[1] === country);

    const totalCases = countryData[countryData.length - 1][4];

    if (totalCases < 75) {
        throw new Error("not enough data");
    }

    const growthRates = countryData.reduce((acc, row, idx) => {
        if (idx === 0) {
            return acc;
        }
        return [...acc, (row[4] - countryData[idx - 1][4]) / countryData[idx - 1][4]]
    }, []);

    const median = stats.median(growthRates);
    const mean = stats.mean(growthRates);

    return { median, mean };
}

const getAllTimesFebruar = () => {
    return Array.from({ length: 29 }, (_, idx) => `2020-02-${idx < 9 ? "0" : ""}${idx + 1}T12:00:00`);
}

const getCapitalAverageTemperatureFebrurary = async ({ longitude, latitude, capital }) => {
    const times = getAllTimesFebruar();
    const uris = times.map(t => `https://api.darksky.net/forecast/${process.env.DARK_SKY_KEY}/${latitude},${longitude},${t}?exclude=daily,flags,minutely,daily,alerts`)

    const temps = []
    const humids = []

    for (const uri of uris) {
        try {
            const { currently: { temperature, humidity } } = await fetchWithCache(uri);
            temps.push(fahrenheitToCelsius(temperature));
            humids.push(humidity);
        } catch (error) {
            console.log(error)
        }
    }

    const averageTemp = stats.mean(temps);
    const averageHumid = stats.mean(humids);

    return [averageTemp, averageHumid];
}

const processCountry = async ({ country, totalCases }, data) => {
    try {
        let capital = overrideCountryCity[country];

        if (!capital) {
            const countryData = wcc.getCountryDetailsByName(country);
            capital = countryData[0].capital;
        }

        // Get the coordinates
        const [{ latitude, longitude, countryCode }] = await geocoder.geocode(`${capital}, ${country}`);

        const { median, mean } = calculateMedianGrowthRate(country, data);
        const [temp, humid] = await getCapitalAverageTemperatureFebrurary({ latitude, longitude });

        return { country, capital, mean, median, temp, humid, totalCases };

    } catch (error) {
        console.log("error for: " + country);
    }


    return null;
}



const parseData = () => {
    return new Promise((r, e) => {
        const data = [];
        const countries = {};
        let idx = 0;
        fs.createReadStream('full_data.csv')
            .pipe(csv.parse())
            .on('data', (row) => {
                if (idx++ === 0) {
                    return;
                }
                // exlude if less than 5 cases
                if (row[4] > 4) {
                    data.push(row);
                    countries[row[1]] = {
                        country: row[1],
                        city: row[6],
                        totalCases: row[4]
                    }
                }
            })
            .on('end', () => {
                return r({
                    data, countries: Object.values(countries)
                });
            });
    });
}

const saveData = async (data) => {
    const csvWriter = createCsvWriter({
        path: 'result.csv',
        header: [
            { id: 'country', title: 'Country' },
            { id: 'capital', title: 'Capital' },
            { id: 'median', title: 'Median growth' },
            { id: 'mean', title: 'Mean growth' },
            { id: 'temp', title: 'Average Temp' },
            { id: 'humid', title: 'Average humidity' },
            { id: 'totalCases', title: 'Total cases' },
        ]
    });

    await csvWriter.writeRecords(data);
}

const processAll = async () => {
    const { data, countries } = await parseData();
    const cc = [...countries];
    let result = [];
    let idx = 0;


    for (country of countries) {
        try {
            const d = await processCountry(country, data);
            // console.log(idx++ + " out of: " + cc.length);
            result.push(d);
        } catch (error) {
            console.log(error);
        }
    }

    result = result.filter(Boolean).filter(row => !isNaN(row.median));
    console.log(result.length + ' countries')

    await saveData(result);
}


processAll();
