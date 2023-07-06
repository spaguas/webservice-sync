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

// var job_daee_sync = new CronJob(
//     process.env.CRONJOB_DAEE,
//     function(){       
        Promise.all(
        [
            //getMeasurements('SAISP',startDt,endDt),
            //getMeasurements('DAEE',startDt,endDt),
            //getMeasurements('ANA',startDt,endDt),
            //getMeasurements('IAC',startDt,endDt),
            //getMeasurements('CEMADEN',startDt,endDt)
        ]).then(res => {
            console.log("Process Finished!!!")
            process.exit(0);
        })

        //process.exit(0);
//     },
//     null,
//     true,
//     'America/Sao_Paulo'
// );

function calculateTimeDifferenceInMinutes(startDate, endDate) {
    var diff = moment.duration(endDate.diff(startDate));
    var diffInMinutes = diff.asMinutes();
    //console.log("Diff Time [",startDate,",",endDate,"] => ", diffInMinutes);  
    return diffInMinutes;
}

async function getMeasurements(station_owner,startDt,endDt){
    //Collect measurements By Station Owner
    db_source.task(async t1 => {
      const measurements = await t1.any("SELECT * FROM measurements WHERE extract(year from datetime) = 2022 and extract(month from datetime) = 6 and station_owner = $1 and syncronized_at is null order by datetime asc limit 1000000", [station_owner]);
      const stations     = await t1.any("SELECT * FROM stations");

      return {measurements, stations};  
    }).then(data => {

        //Grouping Measurements By Prefix
        mds = _.groupBy(data.measurements, function(o){ return o.prefix });

        let station_not_located = [];
        let station_without_data = [];

        forEach(mds, function(measurements,prefix){
            let vals_sibh = [];
            let transmission_gap;
            let total_rainfall = 0;
            let previous_md    = 0;

            //Locate Station using Prefix
            let station = _.first(_.filter(data.stations, function(o){ return o.prefix == prefix }));

            //console.log("Station Located: ", station);
            //Locate Fluviometric and Pluviometric Stations using StationId Field
            //let station_piez = _.filter(data.stations, function(o){ return o.station_id == station.station_id && o.station_type == "Piezométrico"});

            //Check station located
            if(station){
                console.log("Prefix: ", station.prefix," - Qtd. Measurements: ", measurements.length);

                let station_plu  = _.first(_.filter(data.stations, function(o){ return o.station_id == station.station_id && o.station_type == "Pluviométrico"}));
                let station_flu  = _.first(_.filter(data.stations, function(o){ return o.station_id == station.station_id && o.station_type == "Fluviométrico"}));

                //Iterate over measurements to construct values
                forEach(measurements, function(md,k){
                    let ws_origin      = 'WS-'+station_owner;
                    let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
                    let rainfall_event = null;

                    //console.log("Measurement: ", md);

                    //Check if Rainfall data is present
                    if(md.rainfall != null && station_plu){
                        transmission_gap = station_plu.transmission_gap;

                        if(station_owner == 'SAISP'){

                            //let rainfall_value = 0;
                            let hour = parseInt(moment(date_hour_obj).format('HHmm'));

                            //Checking Index of Md
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
                            station_prefix_id: station_plu.station_prefix_id,
                            created_at: md.created_at,
                            updated_at: md.created_at,
                            transmission_gap: station_plu.transmission_gap,
                            measurement_gap: station_plu.measurement_gap
                        })
                    }

                    //Check if Level data is present
                    if(md.level != null && station_flu){
                        transmission_gap = station_flu.transmission_gap;

                        vals_sibh.push({
                            date_hour: date_hour_obj,
                            value: md.level,
                            read_value: md.discharge,
                            battery_voltage: md.battery_level,
                            information_origin: ws_origin,
                            measurement_classification_type_id: 3,
                            transmission_type_id: 4,
                            station_prefix_id: station_flu.station_prefix_id,
                            created_at: md.created_at,
                            updated_at: md.created_at,
                            transmission_gap: station_flu.transmission_gap,
                            measurement_gap: station_flu.measurement_gap
                        });
                    }
                });

                //After looping in Measurements - Lets Insert
                if(vals_sibh.length > 0){
                    const q_sibh = pgp.helpers.insert(vals_sibh, cs_sibh) + " ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO UPDATE SET read_value = measurements.read_value, value = measurements.value, battery_voltage=measurements.battery_voltage RETURNING id, date_hour, created_at;";
                    
                    db_sibh.any(q_sibh).then(result => {
                        //console.log("measurements inserted: ", result);
                        
                        let dates = _.map(result, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });
                        let ids   = _.map(result, function(e){ return e.id });
                        let transmissions = _.map(result, function(e){ return e.created_at } );

                        db_source.any('UPDATE measurements SET syncronized_at=$1 WHERE prefix=$2 and to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($3:list) RETURNING prefix, syncronized_at', 
                        [                
                            moment().add(3,'hours').format('YYYY-MM-DD HH:mm:ss'),
                            prefix,
                            dates
                        ]).then(res => {
                            //console.log("UPDATE RETURN:", res);
                            console.log(prefix," - Measurements Updated: ", dates.length);
                            //console.log("Ids: ", ids);
                            //console.log("Transmissions: ", transmissions);

                            //Update SIBH Stations With Data Returned
                            /*let diffInMinutes = calculateTimeDifferenceInMinutes(moment.utc(_.last(dates)), moment().utc());
                            let transmission_status = (diffInMinutes <= (transmission_gap * 1.25)) ? 0 : 1 ; //25% plus in time to gap transmissions

                            db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE prefix = $5",[
                                transmission_status,
                                _.last(dates),
                                _.last(ids),
                                _.last(transmissions),
                                prefix
                            ]).then(w => {
                                //console.log(prefix, " - Measurements Updated!!! => Períod: [",_.first(dates),",",_.last(dates),"] - Qtd: [ ",_.size(ids)," ]");
                            }).catch(error => {
                                console.error("Error SQL: ", error);
                            });*/
                        });    
                    });
                    //Execute update
                    
                }
            }
            else{
                station_not_located.push(prefix);
            }

        });
        
    }).catch(error_t1 => {
        console.error("Error Task(1):",error_t1);
    }).finally(() =>{
        //process.exit(0);
    });
}