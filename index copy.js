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

let startDt = (process.env.RANGE_COLLECT_DATE_START != "") ? moment(process.env.RANGE_COLLECT_DATE_START) : moment().subtract(process.env.RANGE_COLLECT_HOURS,'hours');
let endDt   = (process.env.RANGE_COLLECT_DATE_END   != "") ? moment(process.env.RANGE_COLLECT_DATE_END)   : moment();

let dateRange = [startDt,endDt];

let station_owner = "SAISP";

//Execute 1 hours
var job_stations_sync = new CronJob(
    process.env.CRONJOB_SYNC_STATIONS,
    function(){   
        loadStationPrefixes();
    },
    function(e){
        console.log("Job Sync Stations Completed")
    },
    true,
    'America/Sao_Paulo'
);

var job_stations_sync = new CronJob(
    process.env.CRONJOB_DAEE,
    function(){   
        syncronizeMeasurements(['SAISP','DAEE'],'WS-SAISP', dateRange, true,null,null," prefix asc, datetime asc");
        syncronizeMeasurements(['IAC'],'WS-IAC', dateRange, true,null,null," prefix asc, datetime asc");
        syncronizeMeasurements(['DAEE'], 'WS-DAEE',dateRange, true,null,null," prefix asc, datetime asc");
        syncronizeMeasurements(['CEMADEN'],'WS-CEMADEN', dateRange, true,null,null," prefix asc, datetime asc");
        syncronizeMeasurements(['ANA'], 'WS-ANA',dateRange, true,null,null," prefix asc, datetime asc");
    },
    function(e){
        console.log("Job Sync Stations Completed")
    },
    true,
    'America/Sao_Paulo'
);


//loadStationPrefixes();
checkMeasurements(24).then(vals => {
    //console.log("All Stations: ", stations);
    //console.log("Vals: ", vals)
    let stations_ok = [];
    _.each(vals.results, function(rst){
        //console.log("Rst: ", rst)
        let station = _.filter(vals.stations, function(o){ 
            if(o.station_owner == 'IAC'){
                let cod_ibge = parseInt(rst.prefix.split("-")[0]);
                let station_name = rst.prefix.split("-")[1];
                let prefix_name  = o.prefix.substring(o.prefix.indexOf("-")+1, o.prefix.indexOf(" - SP"));
                ret = o.city_cod == cod_ibge && station_name == prefix_name;
                
                return ret;
            }
            else{
                return o.prefix == rst.prefix || o.prefix_alt == rst.prefix
            }
        });

        if(station){
            //console.log("Station Finded: ", station);
            stations_ok.push(...station);
        }

    });

    //Process to extract only station_id
    const station_ids = _.uniq(_.map(stations_ok, function(o){ return o.station_id }));

    //console.log("Station Ids: ", station_ids);

    //console.log("Station OKs: ", station_ids);
    db_sibh.task(async tx => {
        up_oks = await tx.any("UPDATE station_prefixes SET transmission_status = 0 WHERE station_id IN ($1:csv) RETURNING id,updated_at", [station_ids]);
        up_noks = await tx.any("UPDATE station_prefixes SET transmission_status = 1 WHERE station_id NOT IN ($1:csv) RETURNING id,updated_at", [station_ids]);

        return {up_oks: up_oks, up_noks: up_noks};
    }).then(events => {
        console.log("Station with transmission OK => ", _.size(events.up_oks));
    });
});


function syncronizeMeasurements(station_owners, ws_owner, dateRange){
    getMeasurements(station_owners,null,dateRange,true,null,null," prefix asc, datetime asc").then(mds => {
    
        //Group by prefix
        mds_grouped = _.groupBy(mds, function(o){ return o.prefix });
    
        const total_stations = _.size(mds_grouped);
    
        //Getting all Stations from Datasource
        getStations(station_owners).then(stations => {
            //Each by prefixes
            _.each(mds_grouped, function(measurements, prefix){
                //console.log(prefix," => ", measurements.length);
                let vals_flu_sibh = [];
                let vals_plu_sibh = [];
                let vals_piezo_sibh = [];

                let total_measurements = _.size(measurements);
    
                //Find station by prefix
                let stations_filtered = _.filter(stations, function(o){
                    if(o.station_owner == 'IAC'){
                        let cod_ibge = parseInt(prefix.split("-")[0]);
                        let station_name = prefix.split("-")[1];
                        let prefix_name  = o.prefix.substring(o.prefix.indexOf("-")+1, o.prefix.indexOf(" - SP"));
                        //console.log("Check IAC StationName:["+station_name+"]["+prefix_name+"]=> ",cod_ibge," <> ",o);
                        return o.city_cod == cod_ibge && station_name == prefix_name;
                    }
                    else{
                        return o.prefix == prefix || (o.prefix_alt != null && o.prefix_alt == prefix);
                    }
                })
    
                if(stations_filtered.length == 0){
                    console.log("Prefix: ",prefix," not located");
                    //Makes update in stations with_no_data
                }

                let station_flu   = _.head(_.filter(stations_filtered, function(o){ return o.station_type.indexOf("Flu") > -1 }));
                let station_plu   = _.head(_.filter(stations_filtered, function(o){ return o.station_type.indexOf("Plu") > -1 }));
                //let station_piezo = _.filter(stations_filtered, function(o){ return o.station_type.indexOf("Piezo") > -1 });
    
                let total_rainfall = 0;
    
                _.each(measurements, function(md,k){
    
                    let ws_origin      = ws_owner;
                    let date_hour_obj  = moment(md.datetime).format("YYYY-MM-DD HH:mm");
                    
                    //console.log(prefix+" - Measurements["+k+"] => date:"+md.datetime+" => ",md.rainfall,",",md.level,",",md.discharge);

                    //Check if rainfall is fill and station_plu exist
                    if(md.rainfall != null && station_plu){
                        total_rainfall += md.rainfall;
    
                        vals_plu_sibh.push({
                            date_hour: date_hour_obj,
                            value: md.rainfall,
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
                        });
                    }
    
                    //Check if level is fill and station_flu exist
                    if(md.level != null && station_flu){
                        vals_flu_sibh.push({
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
    
                //Processing to bulk insert measurements
                let sync_tasks = [];
    
                if(station_plu){
                    if(vals_plu_sibh.length > 0){ 
                        sync_tasks.push(insertBulkMeasurements(vals_plu_sibh));
                    }
                }
    
                if(station_flu){
                    if(vals_flu_sibh.length > 0){
                        sync_tasks.push(insertBulkMeasurements(vals_flu_sibh));
                    }
                }                            
    
                //Execute all sync tasks
                Promise.all(sync_tasks).then(results => {
                   let total_length = 0;

                   _.each(results, function(x){
                    total_length += _.size(x);
                   })

                   console.log(ws_owner+" - Prefix: "+prefix+" - Measurements Inserted: "+total_length+"/"+total_measurements);
                });
            });
        });
    })
}

function checkMeasurements(hours){
    
    return db_source.task(async tk => {
        //Loading all stations
        const stations = await tk.any('SELECT * FROM stations');
        const results  = await tk.any("SELECT prefix, max(datetime) as datetime, max(created_at) as created_at FROM measurements WHERE datetime >= now() - interval '$1 hours' GROUP BY prefix", [hours]);
        
        return {stations: stations, results: results};
    })
}

/**
 * Function to update status from station
 * @param {*} prefix 
 * @param {*} inserted_measurements
 * @param {*} tolerance 
 * @returns 
 */
function updateStationStatusInSibh(station, inserted_measurements, tolerance){
    if(_.size(inserted_measurements) > 0){
        let ids                = _.map(inserted_measurements, function(e){ return parseInt(e.id) });
        let transmissions      = _.map(inserted_measurements, function(e){ return e.created_at } );
        let dates              = _.map(inserted_measurements, function(e){ return moment(e.date_hour).format('YYYY-MM-DD HH:mm') });

        let diffInMinutes = calculateTimeDifference(moment.utc(_.last(dates)), moment().utc()).asMinutes();
        let transmission_status = (diffInMinutes >= 0 && diffInMinutes <= (station.transmission_gap * tolerance)) ? 0 : 1 ; //100% plus in time to gap transmissions

        return db_sibh.any("UPDATE station_prefixes SET transmission_status=$1,date_last_measurement=$2, id_last_measurement=$3, date_last_transmission=$4 WHERE id IN ($5:list) RETURNING id, prefix, updated_at",[
            transmission_status,
            _.last(dates),
            _.last(ids),
            _.last(transmissions),
            station.station_prefix_id
        ]);
    }else{

        return db_sibh.any("UPDATE station_prefixes SET transmission_status=1 WHERE id = $2", [0, station.station_prefix_id])
    }
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
async function getMeasurements(station_owners, prefix, date_range, not_syncronized = true, limit=10000, offset=10000, order_by=" datetime desc"){
    let query = "SELECT * FROM measurements ";
    let conds = [];

    if(station_owners != null){
        let tx = ` station_owner IN ('${station_owners.join("', '")}')`;
        conds.push(tx);
    }

    if(prefix != null){
        conds.push(" prefix = '"+prefix+"'");
    }

    if(date_range != null || date_range.length == 2){
        conds.push(" datetime between '"+date_range[0].format("YYYY-MM-DD HH:mm")+"' and '"+date_range[1].format("YYYY-MM-DD HH:mm")+"' ");
    }

    if(not_syncronized != null){
        conds.push(" syncronized_at is null ");
    }

    if(conds.length > 0){
        query += "WHERE "+conds.join(" and ");
    }

    if(order_by != null){
        query += "ORDER BY "+order_by;
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
 * Method to get stations lists
 * @returns 
 */
async function getStations(station_owners,station_type=null){

    let query = "SELECT * FROM stations";
    let conds = [];

    if(station_owners != null){
        let tx = ` station_owner IN ('${station_owners.join("', '")}')`;
        conds.push(tx);
    }

    if(station_type){
        conds.push(" station_type = '"+station_type+"' ");
    }

    if(conds.length > 0){
        query += " WHERE "+conds.join(" and ");
    }

    return await db_source.any(query);
};

/**
 * Method to bulk insert measurement into SIBH Database
 * @param {*} station_type
 * @param {*} measurements 
 */
 async function insertBulkMeasurements(measurements){
    //let on_conflict = "ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO UPDATE SET read_value = measurements.read_value, value = measurements.value, battery_voltage=measurements.battery_voltage RETURNING id, station_prefix_id, date_hour, created_at;"
    let on_conflict = "ON CONFLICT (station_prefix_id, date_hour, transmission_type_id) DO NOTHING RETURNING id, station_prefix_id, date_hour, created_at;"
    const q_sibh = pgp.helpers.insert(measurements, cs_sibh) + on_conflict;
    return await db_sibh.any(q_sibh);
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
