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
            console.log('Duration:', e.ctx.duration);
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
            console.error("ERROR CONNECTION: ", e);
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

let startDt = (process.env.RANGE_COLLECT_DATE_START != "-") ? moment(process.env.RANGE_COLLECT_DATE_START) : moment().subtract(process.env.RANGE_COLLECT_HOURS,'hours');
let endDt   = (process.env.RANGE_COLLECT_DATE_END   != "-") ? moment(process.env.RANGE_COLLECT_DATE_END)   : moment().add(process.env.RANGE_COLLECT_HOURS,'hours');;

let dateRange = [startDt,endDt];

console.log("Date Start: ", startDt," - DateEnd: ", endDt);




//loadStationPrefixes();

//sync_measurements(null,dateRange,limit,offset," ORDER BY datetime DESC");

var job_retro_sync = new CronJob(
    process.env.CRONJOB_RETRO_SYNC,
    function(){
        
        let limit  = 10000;
        let offset = 0;
        
        let startDt = (process.env.RANGE_COLLECT_DATE_START != "-") ? moment(process.env.RANGE_COLLECT_DATE_START) : moment().subtract(process.env.RANGE_COLLECT_HOURS,'hours');
        let endDt   = (process.env.RANGE_COLLECT_DATE_END   != "-") ? moment(process.env.RANGE_COLLECT_DATE_END)   : moment();
        let dateRange = [startDt,endDt];

        //console.log("Get measurements limit ",limit," offset ", offset);        
        sync_measurements(null,dateRange,limit,offset," ORDER BY datetime DESC");
        offset += limit;
    },
    null,
    true,
    'America/Sao_Paulo'
);
/**
 * Function to check if measurement exist in measurements database
 * @param {*} station_prefix_id 
 * @param {*} datetime 
 */
async function checkMeasurementsExists(station_prefix_id, datetime){
    //db_source.any("SELECT * FROM measurements WHERE prefix")
    return await db_sibh.any("SELECT 1 FROM measurements WHERE station_prefix_id = $1 and to_char(date_hour, \'YYYY-MM-DD HH24:MI\') = $2 LIMIT 1", [station_prefix_id, datetime]);
}

/**
 * Update all Stations Without Measurements
 * @returns None
 */
async function updateStationStatusWithoutMeasurements(){
    return await db_sibh.any("UPDATE station_prefixes SET transmission_status = 0, measurement_status = 0, updated_at = NOW() WHERE date_last_transmission is null");
}

/**
 * Function to get measurements by paramters
 * @param {*} station_owner 
 * @param {*} date_range 
 * @param {*} limit 
 * @param {*} offset 
 * @param {*} order_by 
 */
function sync_measurements(station_owner, date_range, limit,offset, order_by){
    
    //current_progress = multibar.create(100, 0);

    getMeasurements(station_owner,null,date_range,null,limit,offset,order_by).then(measurements => {
    console.log("Total of Measurements: ", measurements.length);

    if(measurements.length == 0){
        offset = 0;
    }

    let mds_grouped = _.groupBy(measurements, 'prefix');
    
    let station_not_located = [];
    let station_without_data = [];

    // note: you have to install this dependency manually since it's not required by cli-progress
    const colors = require('ansi-colors');
    
    const stations_progress = new cliProgress.SingleBar({
        format: '{task_name} |' + colors.cyan('{bar}') + '| {percentage}% || {value}/{total} syncronized',
        position: 'right',
        stopOnComplete: true,
    },cliProgress.Presets.shades_grey);

    const measurements_progress = new cliProgress.SingleBar({
        format: '{task_name} |' + colors.cyan('{bar}') + '| {percentage}% || {value}/{total} syncronized',
        position: 'right',
        stopOnComplete: true,
    },cliProgress.Presets.shades_grey);

    getStations(station_owner).then(stations => {
        let i = 1;

        stations_progress.start(mds_grouped.length, 0, {
            task_name: "Loading stations",
            total: mds_grouped.length,
        });
           
        
        const total_stations = _.size(mds_grouped);
        //current_progress.setTotal(total_stations);

        _.each(mds_grouped, function(mds,prefix){
            let per = (i*100)/total_stations;

            let vals_flu_sibh = [];
            let vals_plu_sibh = [];

            let filter_stations = _.filter(stations, function(o){ 
                if(station_owner == 'IAC'){

                    //Prefix from Webservice-Data is Filled with IBGE-Name
                    let cod_ibge = prefix.split("-")[0];
                    let prefix_name = prefix.split("-")[1];
                    let station_name = o.name;

                    return o.city_cod == cod_ibge;
                }else{
                    return o.prefix == prefix || o.prefix_alt == prefix }
            });

            //console.log("Stations Filtered: ", filter_stations," => Stations: ", stations.length);

            let station = _.first(filter_stations);

            let total_rainfall = 0;
            let previous_md    = 0;

            //console.log("Station Object: ", station);

            if(station != undefined){
                //
                let station_plu  = _.first(_.filter(stations, function(o){ return o.station_id == station.station_id && o.station_type == "Pluviométrico"}));
                let station_flu  = _.first(_.filter(stations, function(o){ return o.station_id == station.station_id && o.station_type == "Fluviométrico"}));

                if(station_flu){
                    //calculateDischarges(station_flu.station_id, mds);
                }

                //Iterate over measurements and insert to sibh
                _.each(mds, function(md,k){
                    
                    let ws_origin      = 'WS-'+station_owner;
                    let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
                    let rainfall_event = null;                   

                    let per_mds = (k*100)/mds.length;

                    //measurements_progress.update(per_mds);

                    if(md.rainfall != null && station_plu){
                        //console.log("Pluviometric measurement : ", station_plu.station_prefix_id, " - Measurement: ", md);

                        //checkMeasurementsExists(station_plu.station_prefix_id, date_hour_obj);

                        if(station_owner == 'SAISP'){
                            transmission_gap = station_plu.transmission_gap;

                            //let rainfall_value = 0;
                            let hour = parseInt(moment(date_hour_obj).format('HHmm'));

                            if(k > 0){
                                previous_md = measurements[k-1].rainfall;
                                if(previous_md == null || hour == 1010){
                                    previous_md = md.rainfall;
                                }
                            }
                            
                            let rf = Math.abs(md.rainfall-previous_md);
                            rainfall_event = (isNaN(rf) || rf < 0 || rf == null) ? 0 : rf;
                                                                    
                            total_rainfall += rainfall_event;
                        }
                        else{
                            rainfall_event = md.rainfall;
                            total_rainfall += md.rainfall;
                        }
                        
                        vals_plu_sibh.push({
                            date_hour: date_hour_obj,
                            value: rainfall_event,
                            read_value: total_rainfall,
                            battery_voltage: md.battery_level,
                            information_origin: ws_origin,
                            measurement_classification_type_id: 3,
                            transmission_type_id: 4,
                            //station_prefix_id: station_plu.id,
                            station_prefix_id: station_plu.station_prefix_id,
                            created_at: md.created_at,
                            updated_at: md.created_at,
                            transmission_gap: station_plu.transmission_gap,
                            measurement_gap: station_plu.measurement_gap
                        })
                    }

                    if(md.level != null && station_flu){
                        //console.log("Fluviometric measurement : ", station_flu.station_prefix_id, " - Measurement: ", md);
                        // console.log("key Curve: ", getKeyCurves(station_flu.station_id));

                        transmission_gap = station_flu.transmission_gap;

                        vals_flu_sibh.push({
                            date_hour: date_hour_obj,
                            value: md.level,
                            read_value: md.discharge,
                            battery_voltage: md.battery_level,
                            information_origin: ws_origin,
                            measurement_classification_type_id: 3,
                            transmission_type_id: 4,
                            //station_prefix_id: station_flu.id,
                            station_prefix_id: station_flu.station_prefix_id,
                            created_at: md.created_at,
                            updated_at: md.created_at,
                            transmission_gap: station_flu.transmission_gap,
                            measurement_gap: station_flu.measurement_gap
                        });
                    }
                });

                let sync_task = [];

                if(station_plu){
                    console.log("Station: ["+station_plu.station_id+"] => Prefix: ["+station_plu.prefix+"] - Rainfalls: ", vals_plu_sibh.length);

                    if(vals_plu_sibh.length > 0){
                        sync_task.push(insertBulkMeasurements(vals_plu_sibh));
                    }
                }

                if(station_flu){
                    console.log("Station: ["+station_flu.station_id+"] => Prefix: ["+station_flu.prefix+"] - Level/Discharge: ", vals_flu_sibh.length);

                    if(vals_flu_sibh.length > 0){
                        sync_task.push(insertBulkMeasurements(vals_flu_sibh));
                    }
                }

                Promise.all(sync_task).then(results => {
                    let total_length = _.sumBy(results,'length');
                    let total_mds    = vals_flu_sibh.length + vals_plu_sibh.length;

                    //console.log(prefix, " - Results => ",total_length," => [",vals_plu_sibh.length,"]+[",vals_flu_sibh.length,"] => ",total_mds);
                    
                    if(results[0] && station_plu){
                        //Update Station Status
                        updateStationStatusInSibh(results[0], station.transmission_gap).then(update_stations => {
                            let prefixes = _.map(update_stations, function(o){ return o.prefix });
                            console.log("Station Plu Updated in SIBH => ", prefixes);
                        }).catch(update_station_error => {
                            console.error("Error Update Station: ", update_station_error);
                        });
                    }
                    if(results[1] && station_flu){
                        //Update Station Status
                        updateStationStatusInSibh(results[1], station.transmission_gap).then(update_stations => {
                            let prefixes = _.map(update_stations, function(o){ return o.prefix });
                            console.log("Station Flu Updated in SIBH => ", prefixes);
                        }).catch(update_station_error => {
                            console.error("Error Update Station: ", update_station_error);
                        });
                    }
                  
                    //Update measurements if results equals each
                    if(total_length == total_mds){

                        _.each(results, function(rsts,rkey){
                            updateMeasurementsToSyncronized(prefix,rsts).then(update_result => {
                                console.log(prefix, " - Measurement Updated => ", update_result.length,"/",rsts.length);
                            }).catch(updated_measurement_error => {
                                console.error("Error Update Measurements: ", updated_measurement_error);
                            });
                        });

                        //Update all stations without measurements
                        
                    }

                    //console.log("Percentage: ", per);
                })

            }
            else{
                //console.log("Station not located: ", prefix);
                station_not_located.push(prefix);
            }

            stations_progress.update(per);
            i++;
        });

        //process.exit(0);
        /*_.each(station_not_located, function(prefix,key){
            console.log("Station not located: ", prefix);
        });*/

        stations_progress.stop();

        updateStationStatusWithoutMeasurements().then(res => {
            console.log("Stations Without Measurements Updated: ", res);
        });
        //console.log("Stations not located: ",station_not_located.length);
    });
   

}).catch(error => {
    console.error("Error measurements : ", error);
});

}


/**
 * Method to get stations lists
 * @returns 
 */
async function getStations(station_owner,station_type=null){

    let query = "SELECT * FROM stations";
    let conds = [];

    if(station_owner){
        conds.push(" station_owner = '"+station_owner+"' ");
    }

    if(station_type){
        conds.push(" station_type = '"+station_type+"' ");
    }

    if(conds.length > 0){
        query += " WHERE "+conds.join(" and ");
    }

    //console.log("Query: ", query);

    return await db_source.any(query);
};

/**
 * Method to bulk insert measurement into SIBH Database
 * @param {*} measurements 
 */
async function insertBulkMeasurements(measurements){
    let on_conflict = "ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO UPDATE SET read_value = measurements.read_value, value = measurements.value, battery_voltage=measurements.battery_voltage RETURNING id, station_prefix_id, date_hour, created_at;"
    const q_sibh = pgp.helpers.insert(measurements, cs_sibh) + on_conflict;
    return await db_sibh.any(q_sibh);
}

/**
 * Method to update station status baseated in inserted measurements
 * @param {*} inserted_measurements 
 * @param {*} transmission_gap 
 * @returns 
 */
async function updateStationStatusInSibh(inserted_measurements,transmission_gap){
    let station_prefix_ids = _.map(inserted_measurements, function(e){ return parseInt(e.station_prefix_id) });
    let ids                = _.map(inserted_measurements, function(e){ return parseInt(e.id) });
    let transmissions      = _.map(inserted_measurements, function(e){ return e.created_at } );
    let dates              = _.map(inserted_measurements, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });

    let diffInMinutes = calculateTimeDifference(moment.utc(_.last(dates)), moment().utc()).asMinutes();
    let transmission_status = (diffInMinutes <= (transmission_gap * 2)) ? 1 : 0 ; //100% plus in time to gap transmissions

    return await db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE id IN ($5:list) RETURNING id, prefix, updated_at",[
        transmission_status,
        _.last(dates),
        _.last(ids),
        _.last(transmissions),
        _.uniq(station_prefix_ids)
    ]);
}

/**
 * Function to update syncronized_at field in Webservice Database
 * @param {*} prefix 
 * @param {*} inserted_measurements 
 */
async function updateMeasurementsToSyncronized(prefix, inserted_measurements){
    //Collect results from bulk insert of measurements
    let dates = _.map(inserted_measurements, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
    
    return await db_source.any('UPDATE measurements SET syncronized_at=$1 WHERE prefix=$2 and to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($3:list) RETURNING prefix, datetime, syncronized_at', 
    [                
        moment().add(3,'hours').format('YYYY-MM-DD HH:mm:ss'),
        prefix,
        dates
    ]);
}

/**
 * Function to get measurements from Webservice Database
 * @param {*} station_owner 
 * @param {*} prefix 
 * @param {*} date_range 
 * @param {*} not_syncronized 
 * @param {*} limit 
 * @returns 
 */
async function getMeasurements(station_owner, prefix, date_range, not_syncronized = true, limit=10000, offset=10000, order_by=" datetime desc"){
    let query = "SELECT * FROM measurements ";
    let conds = [];

    if(station_owner){
        conds.push(" station_owner = '"+station_owner+"'");
    }

    if(prefix){
        conds.push(" prefix = '"+prefix+"'");
    }

    if(date_range){
        conds.push(" datetime between '"+date_range[0].format("YYYY-MM-DD HH:mm")+"' and '"+date_range[1].format("YYYY-MM-DD HH:mm")+"' ");
    }

    if(not_syncronized){
        conds.push(" syncronized_at is null ");
    }

    if(conds.length > 0){
        query += "WHERE "+conds.join(" and ");
    }

    if(order_by){
        query += order_by;
    }

    if(limit){
        query += " limit "+limit;

        if(offset){
            query += " offset "+offset;
        }
    }
    
    console.log(query);

    return await db_source.any(query);
}

/**
 * Function to calculate Difference Time in Minutes
 * This is duration moment object to get results
 * 
 * asSeconds
 * asMinutes
 * asDays
 * asMonths
 * asyears
 * 
 * @param {*} startDate 
 * @param {*} endDate 
 * @returns 
 */
function calculateTimeDifference(startDate, endDate, unit) {
    return moment.duration(endDate.diff(startDate));
}

function loadKeyCurves(){
    axios({
        method: "get",
        url: (process.env.SIBH_API_ENDPOINT+"key_curves"),
        config:{
            headers: {
                user_email: "diego.monteiro@daee.sp.gov.br",
                user_token: "jy98WB2XzqndZUtyxkvr"
            }
        }
    }).then(res => {
        
        //Group Key Curves By Prefix
        keys_grouped = _.groupBy(res.data, 'station_id');

        console.log(keys_grouped);

    }).catch(error => {
        console.error(error);
    });
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
        
        let station_prefixes = res.data;

        let cemaden_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'CEMADEN'
        });

        let saisp_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'SAISP'
        });

        let iac_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'IAC'
        });

        let daee_stations = _.filter(station_prefixes, function(o){
            return o.station_owner.name == 'DAEE'
        });

        let ana_stations = _.filter(station_prefixes, function(o){
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
                transmission_gap: station.transmission_gap,
                measurement_gap: station.measurement_gap
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
                transmission_gap: station.transmission_gap,
                measurement_gap: station.measurement_gap
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
                transmission_gap: station.transmission_gap,
                measurement_gap: station.measurement_gap
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
                transmission_gap: station.transmission_gap,
                measurement_gap: station.measurement_gap
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
                station_owner: 'ANA',
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
                transmission_gap: station.transmission_gap,
                measurement_gap: station.measurement_gap
            });
        });

       
        const q_stations = pgp.helpers.insert([].concat(vals_stations_ana, vals_stations_cemaden, vals_stations_daee, vals_stations_iac, vals_stations_saisp),cs_stations)+ " ON CONFLICT (prefix, station_type, station_owner) DO UPDATE SET name = stations.name, latitude = stations.latitude, longitude = stations.longitude, altitude = stations.altitude, updated_at = NOW() RETURNING prefix, updated_at";     
        db_source.any(q_stations ).then(data => {
            console.log("Stations Inserted: ", data.length);
        });
        

    }).catch(error => {
        console.error(error);
    });
}
