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

console.log("Environments Variables Loaded!");

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
            console.log('Duration(ms):', e.ctx.duration);
            if (e.ctx.success) {
                // e.ctx.result = resolved data;
            } else {
                // e.ctx.result = error/rejection reason;
            }
        } else {
            // this is a task->start event;
            console.log('Start Time:', e.ctx.start);
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
            console.error("ERROR TASK: ", e.ctx);
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
            console.log("Stations Lazy: ",res.noks.length," - Oks: ", res.oks.length);
        });
    },
    null,
    true,
    'America/Sao_Paulo');

//Execute 1 hours
var job_measurements_per_hours_sync = new CronJob(
    process.env.CRONJOB_DAEE,
    async function(){   
        getMeasurementsByHours(500).then(mds => {
            console.log("Measurements: ", _.size(mds.measurements));
            let mds_grouped = _.groupBy(mds.measurements, function(o){ return o.prefix });
        
            //console.log("Measurements Group: ", Object.keys(mds_grouped));
        
            getAllStations().then((stations) => {
                //console.log("Stations: ", list_stations.stations);
                //let stations = _.groupBy(list_stations.stations, function(o){ return o.prefix });
        
                console.log("Total of Stations: ", _.size(stations));
                
                _.forEach(mds_grouped, function(measurements, prefix){
                    
                    //console.log("Stations:", stations)
                    //console.log("Get First Item: ", _.filter(list_stations.stations, function(o){ return (o.prefix_alt != null) ? o.prefix_alt == prefix : o.prefix == prefix }));
                    let station_plu = _.first(_.filter(stations, function(o){ return (o.prefix == prefix || o.alt_prefix == prefix) && o.station_type_id == '2'}))
                    let station_flu = _.first(_.filter(stations, function(o){ return (o.prefix == prefix || o.alt_prefix == prefix) && o.station_type_id == '1'}))

                    let vals_flu_sibh = [];
                    let vals_plu_sibh = [];
                    let total_rainfall = 0;
                    let total_measurements = _.size(measurements);
        
                    _.each(measurements, function(md, k){
        
                        let ws_origin      = "WS-SYNC";
                        let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
                        
                        //console.log(prefix+" - Measurements["+k+"] => date:"+md.datetime+" => ",md.rainfall,",",md.level,",",md.discharge);
        
                        //Check if rainfall is fill and station_plu exist
                        if(md.rainfall != null && !_.isEmpty(station_plu)){
                                                        
                            vals_plu_sibh.push({
                                date_hour: date_hour_obj,
                                value: md.rainfall,
                                read_value: total_rainfall,
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

                            total_rainfall += md.rainfall;
                        }
        
                        //Check if level is fill and station_flu exist
                        if(md.level != null && !_.isEmpty(station_flu)){
        
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
                        }
        
                    });
        
                    //Start process the measurements
                    //let sync_tasks = [];
            
                    if(!_.isEmpty(station_plu) && _.isEmpty(station_flu)){
                        if(vals_plu_sibh.length > 0){ 
                            //sync_tasks.push(insertBulkMeasurements(vals_plu_sibh));
                            insertBulkMeasurements(station_plu, vals_plu_sibh, 3).then(results => {
                                                                
                                //console.log("Results["+prefix+"]: ", results);
                                db_source.task(async tk => {
                                    let updateds = await tk.any('UPDATE measurements SET syncronized_at = now() at time zone \'utc\' WHERE level is null AND prefix = $1 AND to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($2:list) RETURNING prefix,datetime,syncronized_at',[prefix,results.inserts]);
                                    return updateds;
                                }).then(up_res => {
                                    console.log("Measurements Syncronized: ", up_res);
                                    console.log(station_plu.prefix," / ",station_plu.alt_prefix," => Measurements Inserted: ", up_res.length+"/"+vals_plu_sibh.length);
                                });
                            });
                        }
                    }
        
                    if(!_.isEmpty(station_flu) && _.isEmpty(station_plu)){
                        if(vals_flu_sibh.length > 0){
                            //sync_tasks.push(insertBulkMeasurements(vals_flu_sibh));
                            insertBulkMeasurements(station_flu, vals_flu_sibh, 3).then(results => {
                                //console.log(station_flu.prefix," => Measurements Inserted: ", results.inserts.length+"/"+vals_flu_sibh.length);
        
                                db_source.task(async tk => {
                                    let updateds = await tk.any('UPDATE measurements SET syncronized_at = now() at time zone \'utc\' WHERE rainfall is null AND prefix = $1 AND to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($2:list) RETURNING prefix,datetime,syncronized_at',[prefix,results.inserts]);
                                    return updateds;
                                }).then(up_res => {
                                    //console.log("Measurements Syncronized: ", up_res.length);
                                    console.log(station_flu.prefix,"/",station_flu.alt_prefix," => Measurements Inserted: ", results.inserts.length+"/"+vals_flu_sibh.length);
                                });
                            });
                        }
                    }

                    //Posto duplo
                    if(!_.isEmpty(station_flu) && !_.isEmpty(station_plu)){
                        if(vals_flu_sibh.length > 0){
                            insertBulkMeasurements(station_flu, vals_flu_sibh, 3).then(results => {
                                //console.log(station_flu.prefix," => Measurements Inserted: ", results.inserts.length+"/"+vals_flu_sibh.length);
        
                                db_source.task(async tk => {
                                    let updateds = await tk.any('UPDATE measurements SET syncronized_at = now() at time zone \'utc\' WHERE prefix = $1 AND to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($2:list) RETURNING prefix,datetime,syncronized_at',[prefix,results.inserts]);
                                    return updateds;
                                }).then(up_res => {
                                    console.log("Measurements Syncronized: ", up_res.length);
                                    console.log(station_flu.prefix,"/",station_flu.alt_prefix," => Measurements Inserted: ", results.inserts.length+"/"+vals_flu_sibh.length);
                                });
                            });
                        }

                        if(vals_plu_sibh.length > 0){ 
                            //sync_tasks.push(insertBulkMeasurements(vals_plu_sibh));
                            insertBulkMeasurements(station_plu, vals_plu_sibh, 3).then(results => {
                                                                
                                //console.log("Results["+prefix+"]: ", results);
                                db_source.task(async tk => {
                                    let updateds = await tk.any('UPDATE measurements SET syncronized_at = now() at time zone \'utc\' WHERE prefix = $1 AND to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($2:list) RETURNING prefix,datetime,syncronized_at',[prefix,results.inserts]);
                                    return updateds;
                                }).then(up_res => {
                                    //console.log("Measurements Syncronized: ", up_res.length);
                                    console.log(station_plu.prefix," / ",station_plu.alt_prefix," => Measurements Inserted: ", up_res.length+"/"+vals_plu_sibh.length);
                                });
                            });
                        }
                    }
                })       
            })
            
        }).catch(error => {
            console.log("Error Generic")
        })
    },
    null,
    true,
    'America/Sao_Paulo'
);

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
        const stations_status = await tk.any('SELECT sp.id, sp.date_last_transmission, sp.transmission_gap, age(now() at time zone \'utc\', sp.date_last_transmission) as diff from station_prefixes as sp where date_last_transmission is not null and transmission_gap is not null');

        let stations_prefixes_ids_ok = [];
        let stations_prefixes_ids_nok = [];

        _.each(stations_status, function(station_status){
            let diff = _.pickBy(station_status.diff, value => value !== null);
            let diff_keys = Object.keys(diff);

            let diff_minutes = 0;
            _.each(diff_keys, function(key){
                if(key == "years"){
                    diff_minutes += (diff.years * 525600);
                }
                else if(key == "months"){
                    diff_minutes += (diff.months * 43800);
                }
                else if(key == "days"){
                    diff_minutes += (diff.days * 1440);
                }
                else if(key == "hours"){
                    diff_minutes += (diff.hours * 60);
                }
                else if(key == "minutes"){
                    diff_minutes += (diff.minutes)
                }
                else if(key == "seconds"){
                    diff_minutes += (diff.seconds/60);
                }
            })

            is_lazy = diff_minutes > (station_status.transmission_gap * 3); //300% of tolerance

            if(is_lazy){
                stations_prefixes_ids_nok.push(station_status.id);
            }
            else{
                stations_prefixes_ids_ok.push(station_status.id);
            }

            console.log("Station Id: ", station_status.id, " - Date: ", station_status.date_last_transmission," - Diff: ", diff_minutes," - Transmissão OK?: ", !is_lazy);
        });

        const oks  = await tk.any("UPDATE station_prefixes SET transmission_status = 0 WHERE id IN ($1:list) RETURNING id",[stations_prefixes_ids_ok]);
        const noks = await tk.any("UPDATE station_prefixes SET transmission_status = 1 WHERE id IN ($1:list) RETURNING id",[stations_prefixes_ids_nok]);

        return {statuses: stations_status, oks: oks, noks: noks}
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
    return db_sibh.task(async tk => {
        let results = await tk.any(q_sibh);
        
        let dates = _.map(results, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
        let insert_dates       = _.map(measurements, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
        let ids                = _.map(measurements, function(e){ return parseInt(e.id) });
        let transmissions      = _.map(measurements, function(e){ return e.created_at } );
        
        //console.log("Results[",prefix,"]: ",_.max(insert_dates));
        let diffInMinutes = calculateTimeDifference(moment.utc(_.max(insert_dates)), moment().utc()).asMinutes();
                
        let transmission_status = (dates.length > 0 && diffInMinutes <= (station.transmission_gap * tolerance)) ? 0 : 1 ; //100% plus in time to gap transmissions

        console.log("Diff Minutes: ", diffInMinutes," - Transmission Gap: ", (station.transmission_gap * tolerance)," - Status: ", transmission_status);

        await tk.none("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE id = $5",[
            transmission_status,
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


/**
 * Function to load Station Prefixes from SIBH Database
 * To get ID linked with prefixes (Plu,Flu,Piez etc..)
 */
function loadStationPrefixes(){
    axios({
        method: "get",
        url: (process.env.SIBH_API_ENDPOINT+"station_prefixes")
    }).then(res => {
        
        station_prefixes = res.data;

        cemaden_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'CEMADEN'
        });

        saisp_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'SAISP'
        });

        iac_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'IAC'
        });

        daee_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'DAEE'
        });

        ana_stations = _.filter(station_prefixes, function(o){
            let owner = o.station_owner.name;
            return (owner != 'CEMADEN' && owner != 'DAEE' && owner != 'IAC' && owner != 'SAISP')
        });

        const vals_stations_cemaden = [];
        const vals_stations_saisp   = [];
        const vals_stations_daee    = [];
        const vals_stations_iac     = [];
        const vals_stations_ana     = [];

        //Insert Camaden Stations
        _.each(cemaden_stations, function(station,key){
            vals_stations_cemaden.push({
                prefix: station.prefix,
                prefix_alt: station.prefix,
                latitude: station.station.latitude,
                longitude: station.station.longitude,
                altitude: station.altitude,
                name: station.station.name,
                station_owner: station.station_owner.name,
                station_operator: station.station_owner.name,
                station_type: station.station_type.name,
                city_name: station.station.city.name,
                city_cod: station.station.city.cod_ibge,
                ugrhi_name: station.station.ugrhi.name,
                ugrhi_cod: station.station.ugrhi.cod,
                subugrhi_name: station.station.subugrhi.name,
                subugrhi_cod: station.station.subugrhi.cod,
                station_id: station.station.id,
                station_prefix_id: station.id,
                not_located: false,
                without_data: false,
                measurement_gap: station.measurement_gap,
                transmission_gap: station.transmission_gap
            });
        });

        _.each(saisp_stations, function(station,key){
            vals_stations_saisp.push({
                prefix: station.prefix,
                prefix_alt: station.alt_prefix,
                latitude: station.station.latitude,
                longitude: station.station.longitude,
                altitude: station.altitude,
                name: station.station.name,
                station_owner: station.station_owner.name,
                station_operator: station.station_owner.name,
                station_type: station.station_type.name,
                city_name: station.station.city.name,
                city_cod: station.station.city.cod_ibge,
                ugrhi_name: station.station.ugrhi.name,
                ugrhi_cod: station.station.ugrhi.cod,
                subugrhi_name: station.station.subugrhi.name,
                subugrhi_cod: station.station.subugrhi.cod,
                station_id: station.station.id,
                station_prefix_id: station.id,
                not_located: false,
                without_data: false,
                measurement_gap: station.measurement_gap,
                transmission_gap: station.transmission_gap
            });
        });

        _.each(iac_stations, function(station,key){
            vals_stations_iac.push({
                prefix: station.prefix,
                prefix_alt: station.station.city.cod_ibge+"-"+station.station.name,
                latitude: station.station.latitude,
                longitude: station.station.longitude,
                altitude: station.altitude,
                name: station.station.name,
                station_owner: station.station_owner.name,
                station_operator: station.station_owner.name,
                station_type: station.station_type.name,
                city_name: station.station.city.name,
                city_cod: station.station.city.cod_ibge,
                ugrhi_name: station.station.ugrhi.name,
                ugrhi_cod: station.station.ugrhi.cod,
                subugrhi_name: station.station.subugrhi.name,
                subugrhi_cod: station.station.subugrhi.cod,
                station_id: station.station.id,
                station_prefix_id: station.id,
                not_located: false,
                without_data: false,
                measurement_gap: station.measurement_gap,
                transmission_gap: station.transmission_gap
            });
        });

        _.each(daee_stations, function(station,key){
            vals_stations_daee.push({
                prefix: station.prefix,
                prefix_alt: station.alt_prefix,
                latitude: station.station.latitude,
                longitude: station.station.longitude,
                altitude: station.altitude,
                name: station.station.name,
                station_owner: station.station_owner.name,
                station_operator: station.station_owner.name,
                station_type: station.station_type.name,
                city_name: station.station.city.name,
                city_cod: station.station.city.cod_ibge,
                ugrhi_name: station.station.ugrhi.name,
                ugrhi_cod: station.station.ugrhi.cod,
                subugrhi_name: station.station.subugrhi.name,
                subugrhi_cod: station.station.subugrhi.cod,
                station_id: station.station.id,
                station_prefix_id: station.id,
                not_located: false,
                without_data: false,
                measurement_gap: station.measurement_gap,
                transmission_gap: station.transmission_gap
            });
        });

        _.each(ana_stations, function(station,key){
            vals_stations_ana.push({
                prefix: station.prefix,
                prefix_alt: station.alt_prefix,
                latitude: station.station.latitude,
                longitude: station.station.longitude,
                altitude: station.altitude,
                name: station.station.name,
                station_owner: station.station_owner.name,
                station_operator: station.station_owner.name,
                station_type: station.station_type.name,
                city_name: station.station.city.name,
                city_cod: station.station.city.cod_ibge,
                ugrhi_name: station.station.ugrhi.name,
                ugrhi_cod: station.station.ugrhi.cod,
                subugrhi_name: station.station.subugrhi.name,
                subugrhi_cod: station.station.subugrhi.cod,
                station_id: station.station.id,
                station_prefix_id: station.id,
                measurement_gap: station.measurement_gap,
                transmission_gap: station.transmission_gap,
                not_located: false,
                without_data: false
            });
        });

        const q_stations = pgp.helpers.insert([].concat(vals_stations_ana, vals_stations_cemaden, vals_stations_daee, vals_stations_iac, vals_stations_saisp),cs_stations)+ " ON CONFLICT (prefix, station_type, station_owner) DO UPDATE SET updated_at = NOW() RETURNING prefix, updated_at";     
        db_source.query(q_stations);

    }).catch(error => {
        console.error("Erro na execução");
    })
}