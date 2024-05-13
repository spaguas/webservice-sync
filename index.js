const axios = require('axios');
const fs = require('fs');
var moment = require('moment');
const { resolve } = require('path');
const { readdir } = require('fs').promises;
const path = require('path');
//const logger = require('./logger');
const { Worker } = require('worker_threads');
const util = require("util");

// Load the full build for lodash.
var _ = require('lodash');
const { forEach, initial } = require('lodash');
const { as } = require('pg-promise');
const { start } = require('repl');
const { clear } = require('console');

// Load cronjob library
var CronJob = require('cron').CronJob;
require('dotenv').config();

const cliProgress = require('cli-progress');
const { query } = require('express');

const winston = require('winston');

// add bars
//const progress_full = multibar.create(100, 0);

/* Webservice Database Info */
const database_user = process.env.DATABASE_USER;
const database_pass = process.env.DATABASE_PASS;
const database_addr = process.env.DATABASE_ADDR;
const database_port = process.env.DATABASE_PORT;
const database_name = process.env.DATABASE_NAME;

/* Database of SIBH (new) */
const database_user_sibh = process.env.DATABASE_USER_SIBH;
const database_pass_sibh = process.env.DATABASE_PASS_SIBH;
const database_addr_sibh = process.env.DATABASE_ADDR_SIBH;
const database_port_sibh = process.env.DATABASE_PORT_SIBH;
const database_name_sibh = process.env.DATABASE_NAME_SIBH;

let LOGGER;

console.log("Environments Variables Loaded!");

initializeLog()

/* Loading config to Pg-Promise to connect to PostgreSQL */
const pgp = require('pg-promise')({
    /* initialization options */
    capSQL: true, // capitalize all generated SQL
    connect(e) {
        
        //console.log('Connected to database:', e.connectionParameters.database);
    },
    disconnect(e){
        //console.log('Disconnected from database: ', e.connectionParameters.database);
    },
    query(e){
        //console.log("QUERY: ",e.query);
    },
    transact(e) {
        if (e.ctx.finish) {
            // this is a transaction->finish event;
            console.log('Duration:', e.ctx.duration);
            if (e.ctx.success) {
                // e.ctx.result = resolved data;
            } else {
                // e.ctx.result = error/rejection reason;
            }
        } else {
            // this is a transaction->start event;
            console.log('Start Time:', e.ctx.start);
        }
    },
    task(e) {
        if (e.ctx.finish) {
            // this is a task->finish event;
            // console.log('Duration(ms):', e.ctx.duration);
            if (e.ctx.success) {
                // e.ctx.result = resolved data;
            } else {
                // e.ctx.result = error/rejection reason;
            }
        } else {
            // this is a task->start event;
            // console.log('Start Time:', e.ctx.start);
        }
    },
    error(err, e) {
        if (e.cn) {
            // this is a connection-related error
            // cn = safe connection details passed into the library:
            //      if password is present, it is masked by #
            console.error("ERROR CONNECTION");
        }

        if (e.query) {
            // query string is available
            if (e.params) {
                // query parameters are available
                console.error("ERROR PARAMS: ", e.ctx);
            }
        }

        if (e.ctx) {
            // occurred inside a task or transaction
            console.error("ERROR TASK: ", e);
        }
    }
});

/* Creating db object to connect Database */
const db_source = pgp({
    connectionString: 'postgres://'+database_user+':'+database_pass+'@'+database_addr+':'+database_port+'/'+database_name,
    max: 60,
    idleTimeoutMillis: 600000,
    keepAlive: true,
    allowExitOnIdle: false,
    application_name: "WS-SYNC"
});

const cs_source = new pgp.helpers.ColumnSet(
    ['prefix','datetime','rainfall','level','battery_level','station_owner'],
    {table: 'measurements'}
);

const cs_stations = new pgp.helpers.ColumnSet([
    'prefix','latitude','longitude','altitude','name','station_owner','station_operator','station_type','city_name','city_cod',
    'ugrhi_name','ugrhi_cod','subugrhi_name','subugrhi_cod','station_id','station_prefix_id','not_located','without_data', 'prefix_alt', 'measurement_gap', 'transmission_gap'
],{table: 'stations'});

const db_sibh = pgp({
    connectionString: 'postgres://'+database_user_sibh+':'+database_pass_sibh+'@'+database_addr_sibh+':'+database_port_sibh+'/'+database_name_sibh
});

const cs_sibh = new pgp.helpers.ColumnSet(
    ['date_hour', 'value', 'read_value', 'battery_voltage', 'information_origin',
    'measurement_classification_type_id', 'transmission_type_id', 'station_prefix_id',
    'created_at', 'updated_at'],{ table: 'measurements' }
);

let startDt = (process.env.RANGE_COLLECT_DATE_START != "") ? moment(process.env.RANGE_COLLECT_DATE_START) : moment().subtract(process.env.RANGE_COLLECT_HOURS,'hours');
let endDt   = (process.env.RANGE_COLLECT_DATE_END   != "") ? moment(process.env.RANGE_COLLECT_DATE_END)   : moment();

let dateRange = [startDt,endDt];






var job_update_status = new CronJob(
    process.env.CRONJOB_UPDATE_STATION_STATUS,
    async function(){
        updateTransmissionStatus().then(res => {
            console.log("Stations Lazy: ",res.noks.length);
        });
    },
    null,
    true,
    'America/Sao_Paulo');

// updateTransmissionStatus().then(res => {
//     console.log("Stations Lazy: ",res.noks.length);
// })

//Execute 1 hours
var job_measurements_per_hours_sync = new CronJob(
    process.env.CRONJOB_DAEE,
    async function(){   
        startSync().then(res=>{

        })
    },
    null,
    true,
    'America/Sao_Paulo'
);

// startSync()


async function startSync(){
    console.log('começando busca de medições');
    getMeasurementsByHours(24).then(mds => {
        console.log("Measurements: ", _.size(mds.measurements));
        let mds_grouped = _.groupBy(mds.measurements, function(o){ return o.prefix });
        let to_register = {plu:[], flu:[]}
    
        //console.log("Measurements Group: ", Object.keys(mds_grouped));
    
        getAllStations().then((stations) => {
            //console.log("Stations: ", list_stations.stations);
            //let stations = _.groupBy(list_stations.stations, function(o){ return o.prefix });
    
            console.log("Total of Stations: ", _.size(stations));
            
            _.forEach(mds_grouped, function(measurements, prefix){

                console.log('Montando medições do prefixo => ' + prefix)
                
                let stations_flu = _.filter(stations, function(o){ return (o.prefix === prefix || o.alt_prefix == prefix) && o.station_type_id == '1'})
                let stations_plu = _.filter(stations, function(o){ return (o.prefix === prefix || o.alt_prefix == prefix) && o.station_type_id == '2'})
                let station_flu, station_plu

                //Check if have association if fluviometric station
                if(stations_flu.length > 0){
                    if(stations_flu.length > 1){
                        station_flu = stations_flu.filter(x=>x.alt_prefix === prefix)[0] || stations_flu.filter(x=>x.prefix === prefix)[0]
                    } else {
                        station_flu = stations_flu[0]
                    }
                    console.log('Prefixo FLU localizado => ' + station_flu.prefix);
                }

                //Check if have association if pluviometric station
                if(stations_plu.length > 0){
                    if(stations_plu.length > 1){
                        station_plu = stations_plu.filter(x=>x.alt_prefix === prefix)[0] || stations_plu.filter(x=>x.prefix === prefix)[0]
                    } else {
                        station_plu = stations_plu[0]
                    }
                    console.log('Prefixo PLU localizado => ' + station_plu.prefix);
                }

                let vals_flu_sibh = [];
                let vals_plu_sibh = [];
                let total_rainfall = 0;
                let total_measurements = _.size(measurements);

                console.log("Measurements Finded: ", total_measurements)
                
                if(station_plu || station_flu){
                    _.each(measurements, function(md, k){
    
                        let ws_origin      = "WS-SYNC";
                        let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
        
                        //Check if rainfall is fill and station_plu exist
                        if(md.rainfall != null){
                            if(station_plu){                                
                                vals_plu_sibh.push({
                                    date_hour: date_hour_obj,
                                    value: md.rainfall,
                                    read_value: null,
                                    battery_voltage: md.battery_level,
                                    information_origin: ws_origin,
                                    measurement_classification_type_id: 3,
                                    transmission_type_id: 4,
                                    station_prefix_id: station_plu.id,
                                    created_at: md.created_at,
                                    updated_at: md.created_at,
                                    transmission_gap: station_plu.transmission_gap,
                                    measurement_gap: station_plu.measurement_gap
                                });
                                // total_rainfall += md.rainfall;
                            } else {
                                to_register.plu.push(prefix)
                                // LOGGER.info('Posto PLU não cadastrado ' + prefix);
                            }
                        }
        
                        //Check if level is fill and station_flu exist
                        if(md.level != null){
                            if(station_flu){
                                vals_flu_sibh.push({
                                    date_hour: date_hour_obj,
                                    value: md.level,
                                    read_value: md.discharge,
                                    battery_voltage: md.battery_level,
                                    information_origin: ws_origin,
                                    measurement_classification_type_id: 3,
                                    transmission_type_id: 4,
                                    station_prefix_id: station_flu.id,
                                    created_at: md.created_at,
                                    updated_at: md.created_at,
                                    transmission_gap: station_flu.transmission_gap,
                                    measurement_gap: station_flu.measurement_gap
                                });
                            } else {
                                to_register.flu.push(prefix)
                                // LOGGER.info('Posto FLU não cadastrado ' + prefix);
                            }
                            
                        }
                    });


                    if(vals_plu_sibh.length > 0){ 
                        //sync_tasks.push(insertBulkMeasurements(vals_plu_sibh));
                        let vals_plu_chunkeds = _.chunk(vals_plu_sibh, process.env.CHUNK_ARRAY_SIZE);

                        vals_plu_chunkeds.forEach(vals_plu_chunk => {
                            insertBulkMeasurements(station_plu, vals_plu_chunk, 3).then(results => {
                                // console.log(results);
                                //console.log("Results["+prefix+"]: ", results);
                                // if(results.length > 0){
                                //     db_source.task(async tk => {
                                //         let updateds = await tk.any('UPDATE measurements SET syncronized_at = now() at time zone \'utc\' WHERE level is null AND prefix = $1 AND to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($2:list) RETURNING prefix,datetime,syncronized_at',[prefix,results.inserts]);
                                //         return updateds;
                                //     }).then(up_res => {
                                //         console.log("Measurements Syncronized: ", up_res);
                                //         console.log(station_plu.prefix," / ",station_plu.alt_prefix," => Measurements Inserted: ", up_res.length+"/"+vals_plu_sibh.length);
                                //     });
                                // }                                
                            });
                        })                            
                    }else{
                        console.log("Without plu measurements")
                    }

                    if(vals_flu_sibh.length > 0){
                        //sync_tasks.push(insertBulkMeasurements(vals_flu_sibh));
                        let vals_flu_chunkeds = _.chunk(vals_flu_sibh, process.env.CHUNK_ARRAY_SIZE);

                        vals_flu_chunkeds.forEach(vals_flu_chunk => {
                            insertBulkMeasurements(station_flu, vals_flu_chunk, 3).then(results => {
                                //console.log(station_flu.prefix," => Measurements Inserted: ", results.inserts.length+"/"+vals_flu_sibh.length);
                                // if(results.length > 0){
                                //     db_source.task(async tk => {
                                //         let updateds = await tk.any('UPDATE measurements SET syncronized_at = now() at time zone \'utc\' WHERE prefix = $1 AND to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($2:list) RETURNING prefix,datetime,syncronized_at',[prefix,results.inserts]);
                                //         return updateds;
                                //     }).then(up_res => {
                                //         //console.log("Measurements Syncronized: ", up_res.length);
                                //         console.log(station_flu.prefix,"/",station_flu.alt_prefix," => Measurements Inserted: ", results.inserts.length+"/"+vals_flu_sibh.length);
                                //     });
                                // }
                            });
                        })
                    }else{
                        console.log("Without flu measurements")
                    }
                }
            })
            
            if(to_register.plu.length > 0){
                TO_ADD_LOG.info('Postos PLU não cadastrados ' + [...new Set(to_register.plu)].length + ' ===> ' + [...new Set(to_register.plu)].join(', '));
            }

            if(to_register.flu.length > 0){
                TO_ADD_LOG.info('Postos FLU não cadastrados ' + [...new Set(to_register.flu)].length + ' ===> ' + [...new Set(to_register.flu)].join(', '));
            }

            
        })
        
    }).catch(error => {
        console.log("Error Generic: ", error)
    })
}

/**
 * Function to get measurements
 * @param {*} hours 
 * @returns 
 */
async function getMeasurementsByHours(hours){
    return db_source.task(async tk => {
        const measurements = await tk.any('SELECT * FROM measurements WHERE syncronized_at is null and datetime >= NOW() - interval \'$1 hours\'', [hours])
        return {measurements: measurements}
    })   
}

async function updateTransmissionStatus(){
    return db_sibh.task(async tk => {
        
        const stations_list = await tk.any('SELECT id, date_last_measurement, transmission_gap, prefix FROM station_prefixes where date_last_measurement is not null');

        let stations_prefixes_ids_ok = [];
        let stations_prefixes_ids_nok = [];

        stations_list.forEach(station=>{
            let m_date = moment(station.date_last_measurement).subtract(3, 'hours')
            let diff = Math.abs(m_date.diff(moment(), 'minutes'))
            if(diff <= 1440 || diff < (station.transmission_gap*3)){
                stations_prefixes_ids_ok.push(station.id)
            } else {
                stations_prefixes_ids_nok.push(station.id)
            }
        })
        
        const oks  = await tk.any("UPDATE station_prefixes SET transmission_status = 0 WHERE id IN ($1:list) RETURNING id",[stations_prefixes_ids_ok]);
        const noks = await tk.any("UPDATE station_prefixes SET transmission_status = 1 WHERE id IN ($1:list) RETURNING id",[stations_prefixes_ids_nok]);

        return {statuses: stations_list, oks: [], noks: noks}
    });
}

/**
 * Method to bulk insert measurement into SIBH Database
 * @param {*} measurements 
 */
function insertBulkMeasurements(station, measurements, tolerance){
    //let on_conflict = "ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO UPDATE SET read_value = EXCLUDED.read_value, value = EXCLUDED.value, battery_voltage=EXCLUDED.battery_voltage RETURNING id, station_prefix_id, date_hour, created_at;"
    let on_conflict = "ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO NOTHING RETURNING id, station_prefix_id, date_hour, created_at;"
    const q_sibh = pgp.helpers.insert(measurements, cs_sibh) + on_conflict;
    //return await db_sibh.any(q_sibh);

    //console.log("Task Query: ", q_sibh);

    return db_sibh.task(async tk => {
        let results = []
        try{
            results = await tk.any(q_sibh);
        } catch(e){
            console.log(e);
            console.log(measurements);
        }
        
        
        if(results.length == 0){return {inserts: []}}

        //console.log("Results: ", results);

        let dates              = _.map(results, function(e){ return e.date_hour });
        let insert_dates       = _.map(results, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
        let ids                = _.map(results, function(e){ return e.id });
        let transmissions      = _.map(results, function(e){ return e.created_at });
        
        //console.log("Results[",prefix,"]: ",_.max(insert_dates));
        // let diffInMinutes = calculateTimeDifference(moment.utc(_.max(insert_dates)), moment().utc()).asMinutes();
                
        // let transmission_status = (dates.length > 0 && diffInMinutes <= (station.transmission_gap * tolerance)) ? 0 : 1 ; //100% plus in time to gap transmissions

        // console.log("Diff Minutes: ", diffInMinutes," - Transmission Gap: ", (station.transmission_gap * tolerance)," - Status: ", transmission_status);

        console.log("Last Id: ", _.max(ids));
        console.log("Last Date: ", _.max(dates));
        console.log("Last Trans: ", _.max(transmissions));

        await tk.none("UPDATE station_prefixes SET date_last_measurement=$1, id_last_measurement=$2, date_last_transmission=$3 WHERE id = $4",[
            _.max(dates),
            _.max(ids),
            _.max(transmissions),
            station.id
        ]);

        return {inserts: insert_dates}
    });
}

function calculateTimeDifference(startDate, endDate, unit) {
    return moment.duration(endDate.diff(startDate));
}

/**
 * Function to Get all Stations from SIBH
 * @returns 
 */
async function getAllStations(){
    
        let stations_query = 'SELECT ';

        stations_query += ' station_prefixes.*, station_owners.name as owner, cities.cod_ibge, '
        stations_query += ' case when station_owners.name = \'IAC\' then cities.cod_ibge || \'-\' || REPLACE(stations.name, \' - SP\',\'\') else station_prefixes.prefix end as prefix'
        stations_query += ' FROM station_prefixes '
        stations_query += 'LEFT JOIN stations ON (stations.id = station_prefixes.station_id)'
        stations_query += 'LEFT JOIN cities ON (cities.id = stations.city_id)'
        stations_query += 'LEFT JOIN ugrhis ON (ugrhis.id = stations.ugrhi_id)'
        stations_query += 'LEFT JOIN subugrhis ON (subugrhis.id = stations.subugrhi_id)'
        stations_query += 'LEFT JOIN station_types ON (station_types.id = station_prefixes.station_type_id)'
        stations_query += 'LEFT JOIN station_owners ON (station_prefixes.station_owner_id = station_owners.id)'
        
        let stations = await db_sibh.any(stations_query);
        return stations;
}


function initializeLog(){
    const myFormat = winston.format.printf(({ level, message, timestamp }) => {
        return `${timestamp} ${level}: ${message}`;
    });
    TO_ADD_LOG = winston.createLogger({
        format: winston.format.combine(
            winston.format.timestamp(),
            myFormat
        ),
        transports: [
            new winston.transports.Console(),
            new winston.transports.File({ filename: 'logs/stations_to_add.log' })
        ]
    });
}