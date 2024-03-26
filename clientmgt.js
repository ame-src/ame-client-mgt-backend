var express = require('express');
var sql = require("mssql");

const winston = require("winston");

const logger = winston.createLogger({
    // Log only if level is less than (meaning more severe) or equal to this
    level: "info",
    // Use timestamp and printf to create a standard log format
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf(
        (info) => `${info.timestamp} ${info.level}: ${info.message}`
      )
    ),
    // Log to the console and a file
    transports: [
      new winston.transports.Console(),
      new winston.transports.File({ filename: "logs/ameclientmgt.log" }),
    ],
 });

const util = require('util')   
 
function log(...msg) {
   logger.log("info", util.format(...msg))
}

const { get } = require('./pool-manager')

var app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

function format_sql(v){

    if(v == null) return "null";

    let t = typeof v;

    if(t == "string"){
        return `'${v.replaceAll("'", "''")}'`;
    }
    else{
        return `${v}`;
    }
}

async function get_seq_num(type) {

    var application = "AMECLIENTMGT";
    var sql = `SELECT s.seq_num FROM rpm_sequencing s WHERE s.type = '${type}' and s.application = '${application}'`;
    
    const pool = await get("read-pool", config);  
    // query to the database and get the records
    let rows = await pool.request().query(sql); 

    if(rows.recordset.length <= 0) throw new Error(`no seq num for: ${application}`);

    sql = `UPDATE s set s.seq_num = s.seq_num + 1 FROM rpm_sequencing s WHERE s.type = '${type}' and s.application = '${application}'`;
    await pool.request().query(sql); 

    return rows.recordset[0]["seq_num"];
}

class HttpError extends Error {
    constructor(code, message) {
      super(message); // (1)
      this.code = code; // (2)
    }
}

// config for your database
var config = {
    user: 'ame-server',
    password: 'cArrington1859{}',
    server: 'database.amemusic.com', 
    database: 'AmeMaster',
    trustServerCertificate: true
};

const app_pool = new sql.ConnectionPool(config);

function register_route_get(app, path, sql_builder){

    app.get(
        path,
        async (req, res) => {

            try {
                const pool = await get("read-pool", config);  
                // query to the database and get the records
                let rows = await pool.request().query(sql_builder(req.params)); 
                // send records as a response
                await res.send(rows.recordset);
            }
            catch(err){
                log("error", err.message);
                await res.status(500).json({message: err.message});
            }
        }
    );
}

var transactions = {}

app.post("/begin-trans/", async (req, res) => {

    try {
        let client_id = req.headers["client-id"]; 
        if(client_id == undefined) throw new HttpError(400, `client-id not defined`);     

        const pool = await get(client_id, config);

        const transaction = pool.transaction();
        if(!(client_id in transactions)){
            transactions[client_id] = [];
        } 
        transactions[client_id].push(transaction);
        await transaction.begin();
        res.send({message:"TRANS BEGIN"});
    }
    catch(err){
        res.status(500).json({message: err.message});
    }
});

app.post("/commit-trans/", async (req, res) => {

    try {
        let client_id = req.headers["client-id"]; 
        if(client_id == undefined) throw new HttpError(400, `client-id not defined`);     
 
        if(!(client_id in transactions)) throw new HttpError(400, `this client never engaged in a transaction`);
        if(transactions[client_id].length <= 0) throw new HttpError(400, `no known transactions for this client`);

        const transaction = transactions[client_id].pop();
        await transaction.commit();
        res.send({message:"TRANS COMMIT"});
    }
    catch(err){
        res.status(500).json({message: err.message});
    }
});

app.post("/rollback-trans/", async (req, res) => {
    try {
        let client_id = req.headers["client-id"]; 
        if(client_id == undefined) throw new HttpError(400, `client-id not defined`);     
 
        if(!(client_id in transactions)) throw new HttpError(400, `this client never engaged in a transaction`);
        if(transactions[client_id].length <= 0) throw new HttpError(400, `no known transactions for this client`);

        const transaction = transactions[client_id].pop();
        await transaction.rollback();
        res.send({message:"TRANS ROLLBACK"});
    }
    catch(err){
        res.status(500).json({message: err.message});
    }
});

function register_route_put_del(app, path, table, where_builder){

    app.put(path, async (req, res) =>{

        try {
            let client_id = req.headers["client-id"]; 
            if(client_id == undefined) throw new HttpError(400, `client-id not defined`);      
            let obj = req.body;

            let where_clause = where_builder(req.params);

            let sql = `update ${table} set ${Object.keys(obj).map((key) => `${key} = ${format_sql(obj[key])}`).join(", ")} where ${where_clause}`; 
            log("info", "EXECSQL:", sql);
            const pool = await get(client_id, config);
            await pool.request().query(sql);
            await res.send({message:"SUCCESS"});
        }
        catch(err){
            log("error", err.message);
            if(err instanceof HttpError){
                await res.status(err.code).json({message: err.message});
            }
            else {
                await res.status(500).json({message: err.message});    
            }       
        }
    });      

    app.delete(path, async (req, res) =>{

        try {
            let client_id = req.headers["client-id"]; 
            if(client_id == undefined) throw new HttpError(400, `client-id not defined`);      
            let obj = req.body;

            let where_clause = where_builder(req.params);

            let sql = `delete ${table} where ${where_clause}`; 
            log("info", "EXECSQL:", sql);
            const pool = await get(client_id, config);
            await pool.request().query(sql);
            await res.send({message:"SUCCESS"});
        }
        catch(err){
            log("error", err.message);
            if(err instanceof HttpError){
                await res.status(err.code).json({message: err.message});
            }
            else {
                await res.status(500).json({message: err.message});    
            }       
        }
    });      
}

function register_route_post(app, path, table, seq_num){

    app.post(path, async (req, res) =>{

        try {
            let client_id = req.headers["client-id"]; 
            if(client_id == undefined) throw new HttpError(400, `client-id not defined`);      
            let body = req.body;
            body[seq_num[0]] = await get_seq_num(seq_num[1]);

            let sql = `insert ${table} (${Object.keys(body).join(", ")}) values (${Object.keys(body).map((key) => `${format_sql(body[key])}`).join(", ")})`; 
            log("info", "EXECSQL:", sql);
            const pool = await get(client_id, config);
            await pool.request().query(sql);
            await res.send({message:"SUCCESS", seq_num:body[seq_num[0]]});
        }
        catch(err){
            log("error", err.message);
            if(err instanceof HttpError){
                await res.status(err.code).json({message: err.message});
            }
            else {
                await res.status(500).json({message: err.message});    
            }       
        }
    });    
}

register_route_get(app,
    "/options/",
    params => 
        `select option_type, option_value, option_caption, is_default, seq_num from rpm_option_list where application = 'AMECLIENTMGT'  
        UNION
        select 'USER' option_type, user_id option_value, full_name option_caption, NULL is_default, NULL seq_num 
        from rpm_user_list where AME_CLIENT_MGT = 'Y' 
        UNION
        SELECT 'COMPONENT_TYPE' option_type, option_value, option_caption, is_default, NULL seq_num 
        FROM rpm_option_list WHERE application = 'AMEHARDWARE' AND option_type = 'COMPONENT_TYPE'
        UNION
        select distinct 'TEMPLATE_ZONES' option_type, 
        cast(zones as varchar(50)) option_value, case when zones = 1 then 'Single Zone' else cast(zones as varchar(50)) + ' Zones' end option_caption, 
        NULL is_default, zones as seq_num from RPM_SYSTEM_PROFILE_TEMPLATE 
        order by option_type, seq_num, option_value`);

register_route_get(app,
    "/business-categories/",
    params =>
    `SELECT DISTINCT industry_id, bus_category_id, bus_category_desc, is_default FROM rpm_system_bus_category ORDER BY bus_category_desc ASC`)

register_route_get(app,
    "/time-zones/",
    params =>
    `SELECT zone_id, zone_desc, utc_bias, dst_bias FROM time_zone ORDER BY zone_id ASC`)

register_route_get(app,
                "/addresses/:client_id",
                params => 
                    `select address_id, company_id, company_type, company_name, branch_type, Branch,
                        Member, Attention, address_1, address_2, address_3, 
                        City, State, Country, Zip,  
                        case when ol.seq_num is not null then ol.seq_num else 4 end as sort_key  
                        from rpm_client_address ca 
                        left outer join rpm_option_list ol on ol.application = 'AMECLIENTMGT' and ol.option_type = 'BRANCH_TYPE' and ol.option_value = ca.branch_type 
                        WHERE company_type = 'C' AND company_id = ${params.client_id} 
                        order by sort_key, address_1`);

register_route_get(app,
    "/suppliers/",
    params => 
        `select cs.supplier_id, ca.address_id, ca.company_id, ca.company_type, ca.company_name, ca.branch_type, ca.Branch,
            ca.Member, ca.Attention, ca.address_1, ca.address_2, ca.address_3, 
            ca.City, ca.State, ca.Country, ca.Zip,  
            case when ol.seq_num is not null then ol.seq_num else 4 end as sort_key  
            from rpm_computer_supplier cs 
            inner join rpm_client_address ca on ca.address_id = cs.address_id 
            left outer join rpm_option_list ol on ol.application = 'AMECLIENTMGT' and ol.option_type = 'BRANCH_TYPE' and ol.option_value = ca.branch_type  
            order by sort_key, address_1`);

register_route_get(app,
"/address/:address_id",
params => 
    `select address_id, company_id, company_type, company_name, branch_type, Branch,
        Member, Attention, address_1, address_2, address_3, 
        City, State, Country, Zip, cast(case when notes is not null then notes else '' end as varchar(MAX)) as Notes, 
        case when ol.seq_num is not null then ol.seq_num else 4 end as sort_key  
        from rpm_client_address ca 
        left outer join rpm_option_list ol on ol.application = 'AMECLIENTMGT' and ol.option_type = 'BRANCH_TYPE' and ol.option_value = ca.branch_type 
        WHERE company_type = 'C' AND address_id = ${params.address_id} 
        order by sort_key, address_1`);

register_route_put_del(app, "/address/:address_id", "RPM_CLIENT_ADDRESS", 
    (params) => `address_id = ${params.address_id}`);

register_route_post(app, "/address/", "RPM_CLIENT_ADDRESS", ["address_id", "ADDRESS"]);

register_route_get(app,
                "/locations/:client_id",
                params => 
                    `select l.location_id, l.address_id
                    FROM rpm_client_location l WHERE l.client_id = ${params.client_id}`);

register_route_get(app,
    "/location/:location_id",
    params => 
        `select l.location_id, l.address_id, l.bill_to_address_id, l.ship_to_address_id, l.bill_to_policy, 
        l.ship_to_policy, l.monthly_service_fee, l.months_per_bill_period, l.bill_ahead_days,
        l.ascap_rate_plan, l.service_start_date, l.qc_call_date, l.season_start, l.season_end,
        l.zone_id, l.system_grace_period, l.prev_bill_through_date, 
        cast(case when l.Notes is not null then l.Notes else '' end as varchar(MAX)) as Notes, 
        l.update_method, l.disc_threshold, l.update_method_reason, l.location_type, 
        l.location_status, b.billed_from_date, b.billed_through_date, b.paid_through_date,
        l.charge_template_id 
        FROM rpm_client_location l 
        INNER JOIN qry_location_billing_dates b ON l.location_id = b.location_id WHERE l.location_id = ${params.location_id}`);

register_route_put_del(app, "/location/:location_id", "RPM_CLIENT_LOCATION", 
        (params) => `location_id = ${params.location_id}`);

register_route_post(app, "/location/", "RPM_CLIENT_LOCATION", ["location_id", "LOCATION"]);

register_route_get(app,
    "/invoices/location2/:location_id",
    (params) => 
        `SELECT distinct ch.invoice_id FROM  RPM_CLIENT_CHARGE ch 
        inner join RPM_CLIENT_INVOICE i on ch.invoice_id = i.invoice_id
        inner join RPM_CLIENT_LOCATION l on ch.location_id2 = l.location_id
        where l.location_id = ${params.location_id} and (i.is_void = 'Y' or i.is_void is null)`);

register_route_get(app,
    "/profiles/:client_id",
    (params) => `SELECT cp.profile_id, cp.client_id, cp.profile_name, cp.is_interactive, cp.is_shared, cp.video_caps, cp.zones,
        cp.min_4k_unit_storage, cp.template_id, pz.recording_type, cp.db_version_num
        FROM rpm_client_profile cp with (nolock) INNER JOIN ame_profile_zone pz with (nolock) ON cp.profile_id = pz.profile_id
        WHERE pz.zone_id = 1 AND cp.client_id = ${params.client_id} ORDER BY cp.profile_id desc`);


        
const server = app.listen(5000, function () {
    const host = server.address().address;
    const port = server.address().port;
    console.log('client-mgt backend listening at http://%s:%s', host, port);
});
