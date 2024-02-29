const axios = require('axios');
const fs = require('fs');
var moment = require('moment');
const { resolve } = require('path');
const { readdir } = require('fs').promises;
const path = require('path');
//const logger = require('./logger');
const { Worker } = require('worker_threads');

// Load the full build for lodash.
var _ = require('lodash');
const { forEach, initial } = require('lodash');
const { as } = require('pg-promise');
const { start } = require('repl');

// Load cronjob library
var CronJob = require('cron').CronJob;
require('dotenv').config();

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
    max: 30,
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
    'ugrhi_name','ugrhi_cod','subugrhi_name','subugrhi_cod','station_id','station_prefix_id','not_located','without_data'
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

/**
 * Start CronJobs
 * */
//loadStationPrefixes()
db_source.any("SELECT * FROM stations WHERE station_owner = $1", ['CEMADEN']).then(stations => {
    getMeasurements('CEMADEN',startDt,endDt,stations);
});

//Getting all DAEE Flu,Plu,Piez Data
// var job_stations = new CronJob(
// 	'* */1 * * *',
// 	function() {
//         loadStationPrefixes();
// 	},
// 	null,
// 	true,
// 	'America/Sao_Paulo'
// );

// //Sync DAEE measurements
// var job_daee_sync = new CronJob(
//     process.env.CRONJOB_DAEE,
//     function(){       
//         db_source.any("SELECT * FROM stations WHERE station_owner = $1", ['DAEE']).then(stations => {
//             getMeasurements('DAEE',startDt,endDt,stations);
//         });
//     },
//     null,
//     true,
//     'America/Sao_Paulo'
// );

// //Sync CEMADEN measurements
// var job_cemaden_sync = new CronJob(
//     process.env.CRONJOB_CEMADEN,
//     function(){
//         db_source.any("SELECT * FROM stations WHERE station_owner = $1", ['CEMADEN']).then(stations => {
//             getMeasurements('CEMADEN',startDt,endDt,stations);
//         });        
//     },
//     null,
//     true,
//     'America/Sao_Paulo'
// );

// var job_iac_sync = new CronJob(
//     process.env.CRONJOB_IAC,
//     function(){
//         db_source.any("SELECT * FROM stations WHERE station_owner = $1", ['IAC']).then(stations => {
//             getMeasurements('IAC',startDt,endDt,stations);
//         });            
//     },
//     null,
//     true,
//     'America/Sao_Paulo'
// );

// var job_saisp_sync = new CronJob(
//     process.env.CRONJOB_SAISP,
//     function(){
//         db_source.any("SELECT * FROM stations WHERE station_owner = $1", ['SAISP']).then(stations => {
//             getMeasurements('SAISP',startDt,endDt,stations);
//         });   
//     },
//     null,
//     true,
//     'America/Sao_Paulo'
// );

// var job_ana_sync = new CronJob(
//     process.env.CRONJOB_ANA,
//     function(){
//         db_source.any("SELECT * FROM stations WHERE station_owner NOT IN ($1:list)", [['SAISP','DAEE','IAC','CEMADEN']]).then(stations => {
//             getMeasurements('ANA',startDt,endDt,stations);
//         });   
//     },
//     null,
//     true,
//     'America/Sao_Paulo'
// );

/**
 * Função que atualiza o campo de data de sincronização
 * @param {*} prefix 
 * @param {*} data 
 */
function updateSyncronizedAt(prefix, data, transmission_gap, measurement_gap){
    if(data.length > 0 && prefix){
        let dates = _.map(data, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
        let ids   = _.map(data, function(e){ return e.id });
        let transmissions = _.map(data, function(e){ return e.created_at } );

        try {
            db_source.task(t => {
                t.any('UPDATE measurements SET syncronized_at=$1 WHERE prefix=$2 and to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($3:list) RETURNING prefix, syncronized_at', 
                [                
                    moment().add(3,'hours').format('YYYY-MM-DD HH:mm:ss'),
                    prefix,
                    dates
                ]);

                //console.log("Updated Executed: ", dates);
            }).catch(error => {
                console.error("ERROR INSIDE TASK: ", error);
            })
        }catch(error){
            console.error("ERROR TASK: ", error);
        }
        /*db_source.task(async t => {
            
            return await t.any('UPDATE measurements SET syncronized_at=$1 WHERE prefix=$2 and to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($3) RETURNING prefix, syncronized_at', 
            [                
                moment().add(3,'hours').format('YYYY-MM-DD HH:mm:ss'),
                prefix,
                dates.join(",")
            ]);
        }).then(e => {           

            console.log("Update measurements => ", e);
            let diffInMinutes = calculateTimeDifferenceInMinutes(moment.utc(_.last(dates)), moment().utc());
            let transmission_status = (diffInMinutes <= (transmission_gap * 1.25)) ? 0 : 1 ; //25% plus in time to gap transmissions

            db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE prefix = $5",[
                transmission_status,
                _.last(dates),
                _.last(ids),
                _.last(transmissions),
                prefix
            ]).then(w => {
                console.log(prefix, " - Measurements Updated!!! => Períod: [",_.first(dates),",",_.last(dates),"] - Qtd: [ ",_.size(dates)," ]");
            }).catch(error => {
                console.error("Error SQL: ", error);
            });
        });*/

    }

    return false;
}

function calculateTimeDifferenceInMinutes(startDate, endDate) {
    var diff = moment.duration(endDate.diff(startDate));
    var diffInMinutes = diff.asMinutes();
    //console.log("Diff Time [",startDate,",",endDate,"] => ", diffInMinutes);  
    return diffInMinutes;
}

/**
 * Function to calculate discharge from measurement
 * @param {*} station 
 * @param {*} measurement 
 * @returns 
 */
function calculateDischarge(station, measurement){
    let key_curves = [];

    let station_ckeys = _.filter(key_curves, function(o){ return o.station_id == station.station_id });
    console.log("Station: ", station.prefix," => With: ",station_ckeys.length," Key Curves");

    let is_valid_level = false;
    let is_valid_date  = false;

    //Check if level in range
    _.forEach(station_ckeys, function(e){
        console.log("Key Curve => ", e);

        //Check if Level is valid for this Key Curve
        //Check if Key Curve in Date Range
        //Check if Key Curve is Valid
        if(e.is_valid){
            if(measurement.level >= e.start_level && measurement.level < e.end_level){
                if(measurement.date_hour >= e.date_start && measurement.date_hour <= e.date_end){
                    return (e.a * Math.pow((measurement.level - e.h0),e.n));
                }
            }
        }
        else{
            console.error("Key Curve => ", e.id, " is invalid!");
        }
    });

    return -1;
}

function getMeasurements(station_owner,startDt,endDt,stations){
    console.log("Loading ",station_owner," Stations: ", stations.length);
    let station_not_located = [];
    let station_without_data = [];

    db_source.task(async t => {
        console.log("Get measurements from date range: ", startDt.format('YYYY-MM-DD HH:mm')," > ",endDt.format('YYYY-MM-DD HH:mm'));

        //const measurements = await t.any("SELECT * FROM measurements WHERE station_owner = $1 and datetime between $2 and $3 order by datetime asc", [station_owner, startDt, endDt]);
        const measurements = await t.any("SELECT * FROM measurements WHERE station_owner = $1 and syncronized_at is null order by datetime asc LIMIT 10000", [station_owner]);
        return { measurements };
    }).then(data => {
        //Grouping Measurements By Prefix
        mds = _.groupBy(data.measurements, function(o){ return o.prefix });

        //console.log("Grouped Mds: ", mds);

        forEach(mds, function(measurements,prefix){

            let total_rainfall = 0;
            let transmission_gap = 10;
            //console.log("Prefix: ", prefix," - Measurements Located: ", measurements.length);

            let station_prefix = _.first(_.filter(stations, function(o){ 
                if(station_owner == 'IAC'){

                    //Prefix from Webservice-Data is Filled with IBGE-Name
                    let cod_ibge = prefix.split("-")[0];
                    let prefix_name = prefix.split("-")[1];
                    let station_name = o.name;

                    return o.city_cod == cod_ibge;
                }
                else{
                    return o.prefix == prefix
                }
            }));

            let vals_sibh = [];
            let previous_md    = 0;

            //console.log("Station Located: ", station_prefix);

            if(station_prefix){

                let station_flu  = _.first(_.filter(stations, function(o){ return o.station_id == station_prefix.station_id && o.station_type == 'Fluviométrico';}));                
                let station_plu  = _.first(_.filter(stations, function(o){ return o.station_id == station_prefix.station_id && o.station_type == 'Pluviométrico';}));

                console.log("Prefix: ",prefix," - total measurements: ", measurements.length);

                //console.log("Prefix Flu: ", station_flu);
                //console.log("Prefix Plu: ", station_plu);

                forEach(measurements, function(md,k){
                    
                    let ws_origin      = 'WS-'+station_owner;
                    let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
                    let rainfall_event = null;
                    

                    console.log(station_prefix.prefix, " - ", md.datetime," - ",md.rainfall," - ",md.level," - ",md.discharge," - ",md.battery_level);       

                    if(md.rainfall != null && station_plu){
                        //console.log("Pluviometric measurement : ", station_plu.station_prefix_id, " - Measurement: ", md);
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
                        
                        vals_sibh.push({
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

                        transmission_gap = station_flu.transmission_gap;

                        vals_sibh.push({
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

                    //console.log(prefix," - ",date_hour_obj," - Chuva(mm): ", md.rainfall,"/",total_rainfall," - Nível(m): ", md.level," - Vazão(m³/s): ",md.discharge," - Bateria(v): ",md.battery_level);
                    //logger.info(prefix+" - "+date_hour_obj+" - Chuva(mm): "+rainfall_event+"/"+total_rainfall.toFixed(2)+" - Nível(m): "+md.level+" - Vazão(m³/s): "+md.discharge+" - Bateria(v): "+md.battery_level);
                });

                //Add bulk insert of station
                if(vals_sibh.length > 0){
                    
                    try {
                        const q_sibh     = pgp.helpers.insert(vals_sibh, cs_sibh) + " ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO UPDATE SET read_value = measurements.read_value, value = measurements.value, battery_voltage=measurements.battery_voltage RETURNING id, date_hour, created_at;";

                        db_sibh.any(q_sibh).then(data => {
                            console.log(station_owner," => "+prefix," - Measurements Inserted => "+data.length+"/"+measurements.length+" => SIBH_NEW");

                                                       
                            //updateSyncronizedAt(prefix,data,vals_sibh[0].transmission_gap,vals_sibh[0].measurement_gap);
                            if(prefix){

                                let dates = _.map(data, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
                                let ids   = _.map(data, function(e){ return e.id });
                                let transmissions = _.map(data, function(e){ return e.created_at } );
                        
                                db_source.any('UPDATE measurements SET syncronized_at=$1 WHERE prefix=$2 and to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($3:list) RETURNING prefix, syncronized_at', 
                                [                
                                    moment().add(3,'hours').format('YYYY-MM-DD HH:mm:ss'),
                                    prefix,
                                    dates
                                ]).then(res => {
                                    console.log("UPDATE RETURN:", res);

                                    //Update SIBH Stations With Data Returned
                                    // let diffInMinutes = calculateTimeDifferenceInMinutes(moment.utc(_.last(dates)), moment().utc());
                                    // let transmission_status = (diffInMinutes <= (transmission_gap * 1.25)) ? 0 : 1 ; //25% plus in time to gap transmissions

                                    // db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE prefix = $5",[
                                    //     transmission_status,
                                    //     _.last(dates),
                                    //     _.last(ids),
                                    //     _.last(transmissions),
                                    //     prefix
                                    // ]).then(w => {
                                    //     console.log(prefix, " - Measurements Updated!!! => Períod: [",_.first(dates),",",_.last(dates),"] - Qtd: [ ",_.size(dates)," ]");
                                    // }).catch(error => {
                                    //     console.error("Error SQL: ", error);
                                    // });
                                });                                                                
                            }
                            
                        }).catch(error => {
                            console.log("Error XPTO: ", error);
                        });
                    }catch(error){
                        console.log("Error Insert Query: ", error);
                    }
                }
                else{
                    //console.log(prefix," - Measurements not found!");
                    station_without_data.push(prefix);
                }
            }
            else{
                console.log(prefix," - Station not Located");
                station_not_located.push(prefix);
            }
        });

        //List Stations Without Data
        if(station_without_data.length > 0){
            db_source.none("UPDATE stations SET without_data = $1, updated_at = NOW() WHERE prefix IN ($2:list)", [
                true,
                station_without_data
            ]);
        }

        if(station_not_located.length > 0){
            db_source.none("UPDATE stations SET not_located = $1, updated_at = NOW() WHERE prefix IN ($2:list)", [
                true,
                station_not_located
            ]);
        }

        process.exit(0);

    }).catch(error => {
        console.error("Error Task Nested: ", error);
    })

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
                without_data: false
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
                without_data: false
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
                without_data: false
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
                without_data: false
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
                not_located: false,
                without_data: false
            });
        });

        const q_stations = pgp.helpers.insert([].concat(vals_stations_ana, vals_stations_cemaden, vals_stations_daee, vals_stations_iac, vals_stations_saisp),cs_stations)+ " ON CONFLICT (prefix, station_type, station_owner) DO UPDATE SET updated_at = NOW() RETURNING prefix, updated_at";     
        db_source.any(q_stations ).then(data => {
            console.log("Stations Inserted: ", data.length);
        });

    }).catch(error => {
        console.error(error);
    });
}

