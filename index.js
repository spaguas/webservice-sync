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
    error(err, e) {
        if (e.cn) {
            // this is a connection-related error
            // cn = safe connection details passed into the library:
            //      if password is present, it is masked by #
            console.error(e);
        }

        if (e.query) {
            // query string is available
            if (e.params) {
                // query parameters are available
                console.error(e.ctx);
            }
        }

        if (e.ctx) {
            // occurred inside a task or transaction
            console.error(e.ctx);
        }
    }
});

/* Creating db object to connect Database */
const db_source = pgp({
    connectionString: 'postgres://'+database_user+':'+database_pass+'@'+database_addr+':'+database_port+'/'+database_name
});

const cs_source = new pgp.helpers.ColumnSet(
    ['prefix','datetime','rainfall','level','battery_level','station_owner'],
    {table: 'measurements'}
);

const db_sibh = pgp({
    connectionString: 'postgres://'+database_user_sibh+':'+database_pass_sibh+'@'+database_addr_sibh+':'+database_port_sibh+'/'+database_name_sibh
});

const cs_sibh = new pgp.helpers.ColumnSet(
    ['date_hour', 'value', 'read_value', 'battery_voltage', 'information_origin',
    'measurement_classification_type_id', 'transmission_type_id', 'station_prefix_id',
    'created_at', 'updated_at'],{ table: 'measurements' }
);

let startDt = (process.env.RANGE_COLLECT_DATE_START != "") ? moment(process.env.RANGE_COLLECT_DATE_START) : moment().subtract(process.env.RANGE_COLLECT_HOURS,'hours');
let endDt   = (process.env.RANGE_COLLECT_DATE_END != "") ? moment(process.env.RANGE_COLLECT_DATE_END) : moment().add(process.env.RANGE_COLLECT_HOURS, 'hours');

/**
 * Start CronJobs
 * */

//Getting all DAEE Flu,Plu,Piez Data
var job_stations = new CronJob(
	'* */1 * * *',
	function() {
        loadStationPrefixes();
	},
	null,
	true,
	'America/Sao_Paulo'
);

var job_station_not_located = new CronJob(
    '* */1 * * *',
    function(){
        /**Cemaden Stations not Located */
        fs.readFile('./stations/cemaden_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            
            console.log("Trying registry not located stations - CEMADEN")

        });

        /**Ana Stations not Located */
        fs.readFile('./stations/ana_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            
            console.log("Trying registry not located stations - ANA")

        });

        /**IAC Stations not Located */
        fs.readFile('./stations/iac_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            
            console.log("Trying registry not located stations - IAC")

        });

        /**Saisp Stations not Located */
        fs.readFile('./stations/saisp_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            
            console.log("Trying registry not located stations - SAISP")

        });
    },
    null,
    true,
    'America/Sao_Paulo'
);

var job_daee_sync = new CronJob(
    process.env.CRONJOB_DAEE,
    function(){
        console.log("Teste de execução")
        fs.readFile('./stations/daee_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            console.log("DAEE Stations: ", stations.length);
            
            if(stations.length > 0){
                console.log("Getting DAEE Stations");
                getMeasurements('DAEE',startDt,endDt,stations);
                //getMeasurements('SAISP',startDt,endDt,stations);
            }
            else{
                console.log("Zero Stations => ", stations.length);
            }
        });
    },
    null,
    true,
    'America/Sao_Paulo'
);

var job_cemaden_sync = new CronJob(
    process.env.CRONJOB_CEMADEN,
    function(){
        fs.readFile('./stations/cemaden_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            console.log("CEMADEN Stations: ", stations.length);
            
            if(stations.length > 0){
                getMeasurements('CEMADEN',startDt,endDt,stations);
            }
        });
    },
    null,
    true,
    'America/Sao_Paulo'
);

var job_iac_sync = new CronJob(
    process.env.CRONJOB_IAC,
    function(){
        fs.readFile('./stations/iac_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            console.log("IAC Stations: ", stations.length);
            
            if(stations.length > 0){
                getMeasurements('IAC',startDt,endDt,stations);
            }
        });
    },
    null,
    true,
    'America/Sao_Paulo'
);

var job_ana_sync = new CronJob(
    process.env.CRONJOB_ANA,
    function(){
        fs.readFile('./stations/ana_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
            console.log("ANA Stations: ", stations.length);
            
            if(stations.length > 0){
                getMeasurements('ANA',startDt,endDt,stations);
            }
        });
    },
    null,
    true,
    'America/Sao_Paulo'
);

var job_saisp_sync = new CronJob(
    process.env.CRONJOB_SAISP,
    function(){
        fs.readFile('./stations/saisp_stations.json', (err, data) => {
            if (err) throw err;
            let stations = JSON.parse(data);
        
            if(stations.length > 0){
                getMeasurements('SAISP',startDt,endDt,stations);
            }
        });
    },
    null,
    true,
    'America/Sao_Paulo'
);

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

        console.log("Last Date to Compare: ", _.last(dates)," <> ", transmission_gap,",",measurement_gap);
        
        db_source.task(t => {
            
            return t.any('UPDATE measurements SET syncronized_at=$1 WHERE prefix=$2 and to_char(datetime, \'YYYY-MM-DD HH24:MI\') IN ($3:csv)', 
            [
                moment().add(3,'hours').format('YYYY-MM-DD HH:mm:ss'),
                prefix,
                dates
            ]);
        }).then(e => {           
            
            let diffInMinutes = calculateTimeDifferenceInMinutes(moment.utc(_.last(dates)), moment().utc());
            let transmission_status = (diffInMinutes < transmission_gap) ? 0 : 1 ;

            db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE prefix = $5",[
                transmission_status,
                _.last(dates),
                _.last(ids),
                _.last(transmissions),
                prefix
            ]).then(w => {
                console.log(prefix, " - Measurements Updated!!! => [",_.last(dates),",",_.last(ids),"]");
            }).catch(error => {
                console.error("Error SQL: ", error);
            });
        });
    }else{
        db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE prefix = $5",[
            1,
            _.last(dates),
            _.last(ids),
            _.last(transmissions),
            prefix
        ]).then(w => {
            console.log(prefix, " - Measurements Updated!!! => [",_.last(dates),",",_.last(ids),"]");
        }).catch(error => {
            console.error("Error SQL: ", error);
        });
    }

    

    return false;
}

function calculateTimeDifferenceInMinutes(startDate, endDate) {
    var diff = moment.duration(endDate.diff(startDate));
    var diffInMinutes = diff.asMinutes();
    console.log("Diff Time [",startDate,",",endDate,"] => ", diffInMinutes);  
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

    db_source.tx(async t => {
        console.log("Get measurements from date range: ", startDt.format('YYYY-MM-DD HH:mm')," > ",endDt.format('YYYY-MM-DD HH:mm'));

        const measurements = await t.any("SELECT * FROM measurements WHERE station_owner = $1 and datetime between $2 and $3 order by datetime asc", [station_owner, startDt, endDt]);
        return { measurements };
    }).then(data => {
        //Grouping Measurements By Prefix
        mds = _.groupBy(data.measurements, function(o){ return o.prefix });

        forEach(mds, function(measurements,prefix){

            let total_rainfall = 0;
            
            let station_prefix = _.first(_.filter(stations, function(o){ 
                if(station_owner == 'IAC'){

                    //Prefix from Webservice-Data is Filled with IBGE-Name
                    let cod_ibge = prefix.split("-")[0];
                    let prefix_name = prefix.split("-")[1];
                    let station_name = o.station.name;

                    return o.station.city.cod_ibge == cod_ibge;
                }
                else{
                    return o.prefix == prefix || o.alt_prefix == prefix 
                }
            }));

            let vals_sibh = [];
            let previous_md    = 0;

            if(station_prefix){

                let station_flu  = _.first(_.filter(stations, function(o){ return o.station.id == station_prefix.station.id && o.station_type_id == 1;}));                
                let station_plu  = _.first(_.filter(stations, function(o){ return o.station.id == station_prefix.station.id && o.station_type_id == 2;}));

                console.log("Prefix: ",prefix," - total measurements: ", measurements.length);
                forEach(measurements, function(md,k){
                    
                    let ws_origin      = 'WS-'+station_owner;
                    let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
                    let rainfall_event = null;

                    //console.log(station_prefix.station.id, " - ", md.datetime," - ",md.rainfall," - ",md.level," - ",md.discharge," - ",md.battery_level);                    
                    if(md.rainfall != null && station_plu){
                        

                        if(station_owner == 'SAISP'){

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
                            station_prefix_id: station_plu.id,
                            created_at: md.created_at,
                            updated_at: md.created_at,
                            transmission_gap: station_plu.transmission_gap,
                            measurement_gap: station_plu.measurement_gap
                        })
                    }

                    if(md.level != null && station_flu){
                       
                        vals_sibh.push({
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

                    //console.log(prefix," - ",date_hour_obj," - Chuva(mm): ", md.rainfall,"/",total_rainfall," - Nível(m): ", md.level," - Vazão(m³/s): ",md.discharge," - Bateria(v): ",md.battery_level);
                    //logger.info(prefix+" - "+date_hour_obj+" - Chuva(mm): "+rainfall_event+"/"+total_rainfall.toFixed(2)+" - Nível(m): "+md.level+" - Vazão(m³/s): "+md.discharge+" - Bateria(v): "+md.battery_level);
                });

                //Add bulk insert of station
                if(vals_sibh.length > 0){
                    
                    const q_sibh     = pgp.helpers.insert(vals_sibh, cs_sibh) + " ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO UPDATE SET read_value = excluded.read_value, value = excluded.value, battery_voltage=excluded.battery_voltage RETURNING id, date_hour, created_at;";

                    db_sibh.any(q_sibh).then(data => {
                        console.log(station_owner," => "+prefix," - Measurements Inserted => "+data.length+"/"+measurements.length+" => SIBH_NEW");

                        if(data.length > 0){
                            
                            updateSyncronizedAt(prefix,data,vals_sibh[0].transmission_gap,vals_sibh[0].measurement_gap);
                        }
                    });

                   
                }
                else{
                    console.log(prefix," - Measurements not found!");
                    station_without_data.push(prefix);
                }
            }
            else{
                console.log(prefix," - Station not Located");
                station_not_located.push(prefix);
            }
        });

        
        try{
            fs.writeFile('./stations/not_located/'+station_owner.toLowerCase()+'_stations.json', JSON.stringify(station_not_located, null, 2), (err) => {
                if (err){
                    console.log("Erro NotLocated: ", err);
                };
                console.log("Stations from ",station_owner," not Located");
            });
        }catch (err) {
            console.error("Erro de permissão do arquivo not_located...");
        };

        try{
            fs.writeFile('./stations/without_data/'+station_owner.toLowerCase()+'_stations.json', JSON.stringify(station_without_data, null, 2),(err) => {
                if (err){
                    console.log("Erro WithoutData: ", err);
                };
                console.log("Stations from ",station_owner," without data");
            });
        }catch(err){
            console.error("Erro de permissão do without_data...");
        }

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

        fs.writeFile('./stations/daee_stations.json', JSON.stringify(daee_stations, null, 2), (err) => {
            if (err) throw err;
            console.log('DAEE Stations written to file');
        });

        fs.writeFile('./stations/cemaden_stations.json', JSON.stringify(cemaden_stations, null, 2), (err) => {
            if (err) throw err;
            console.log('CEMADEN Stations written to file');
        });

        fs.writeFile('./stations/saisp_stations.json', JSON.stringify(saisp_stations, null, 2), (err) => {
            if (err) throw err;
            console.log('SAISP Stations written to file');
        });

        fs.writeFile('./stations/iac_stations.json', JSON.stringify(iac_stations, null, 2), (err) => {
            if (err) throw err;
            console.log('IAC Stations written to file');
        });

        fs.writeFile('./stations/ana_stations.json', JSON.stringify(ana_stations, null, 2), (err) => {
            if (err) throw err;
            console.log('ANA Stations written to file');
        });

    }).catch(error => {
        console.error(error);
    });
}

