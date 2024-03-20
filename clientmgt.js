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

async function get_seq_num(type) {

    var application = "AMECLIENTMGT";
    var sql = `SELECT seq_num FROM rpm_sequencing WHERE type = '${type}' and application = '${application}'`;
    
    const pool = await get("read-pool", config);  
    // query to the database and get the records
    let rows = await pool.request().query(sql); 

    if(rows.recordset.length <= 0) throw new Error(`no seq num for: ${application}`);

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

function register_route(app, path, sql_builder){

    app.get(
        path,
        async (req, res) => {

            try {
                const pool = await get("read-pool", config);  
                // query to the database and get the records
                let rows = await pool.request().query(sql_builder(req.params)); 
                // send records as a response
                res.send(rows.recordset);
            }
            catch(err){
                res.status(500).json({message: err.message});
            }
        }
    );
}

transactions = {}

app.post("/begin-trans/", async (req, res) => {

    try {
        let client_id = req.headers["client-id"]; 
        if(client_id == undefined) throw new HttpError(400, `client-id not defined`);     

        const pool = await get("read-pool", config); 
        const transaction = pool.transaction();
        transactions[client_id] = transaction;
        await transaction.begin();
        res.send({message:"BEGUN"});
    }
    catch(err){
        res.status(500).json({message: err.message});
    }
});

app.post("/commit-trans/", async (req, res) => {

    try {
        let client_id = req.headers["client-id"]; 
        if(client_id == undefined) throw new HttpError(400, `client-id not defined`);     
 
        const transaction = transactions[client_id];
        await transaction.commit();
        res.send({message:"COMMITTED"});
    }
    catch(err){
        res.status(500).json({message: err.message});
    }
});

app.post("/rollback-trans/", async (req, res) => {
    try {
        let client_id = req.headers["client-id"]; 
        if(client_id == undefined) throw new HttpError(400, `client-id not defined`);     
 
        const transaction = transactions[client_id];
        await transaction.rollback();
        res.send({message:"ROLLED BACK"});
    }
    catch(err){
        res.status(500).json({message: err.message});
    }
});




function register_routes_put_del(app, path, table, where_builder){

    app.put(path, (req, res) =>{

        try {
            let client_id = req.headers["client-id"]; 
            if(client_id == undefined) throw new HttpError(400, `client-id not defined`);      
            let obj = req.body;

            let where_clause = where_builder(req.params);

            let sql = `update ${table} set ${Object.keys(obj).map((key) => `${key} = ${format_sql(obj[key])}`).join(", ")} where ${where_clause}`; 
            log("info", "EXECSQL:", sql);
            res.send(sql);
        }
        catch(err){
            if(err instanceof HttpError){
                res.status(err.code).json({message: err.message});
            }
            else {
                res.status(500).json({message: err.message});    
            }       
        }
    });      

    app.delete(path, (req, res) =>{

        try {
            let client_id = req.headers["client-id"]; 
            if(client_id == undefined) throw new HttpError(400, `client-id not defined`);      
            let obj = req.body;

            let where_clause = where_builder(req.params);

            let sql = `delete ${table} where ${where_clause}`; 
            log("info", "EXECSQL:", sql);
            res.send(sql);
        }
        catch(err){
            if(err instanceof HttpError){
                res.status(err.code).json({message: err.message});
            }
            else {
                res.status(500).json({message: err.message});    
            }       
        }
    });      
}

function register_routes_post(app, path, table, seq_num){

    app.post(path, async (req, res) =>{

        try {
            let client_id = req.headers["client-id"]; 
            if(client_id == undefined) throw new HttpError(400, `client-id not defined`);      
            let body = req.body;
            body[seq_num[0]] = await get_seq_num(seq_num[1]);

            let sql = `insert ${table} (${Object.keys(body).join(", ")}) values (${Object.keys(body).map((key) => `${format_sql(body[key])}`).join(", ")})`; 
            log("info", "EXECSQL:", sql);
            res.send(sql);
        }
        catch(err){
            if(err instanceof HttpError){
                res.status(err.code).json({message: err.message});
            }
            else {
                res.status(500).json({message: err.message});    
            }       
        }
    });    
}

register_route(app,
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

register_route(app,
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

register_routes_put_del(app, "/address/:address_id", "RPM_CLIENT_ADDRESS", 
    (params) => `address_id = ${params.address_id}`);

register_routes_post(app, "/address/", "RPM_CLIENT_ADDRESS", ["address_id", "ADDRESS"]);

function format_sql(v){
    let t = typeof v;

    if(t == "string"){
        return `'${v.replace("'", "''")}'`;
    }
    else{
        return `${v}`;
    }
}

register_route(app,
                "/locations/:client_id",
                params => 
                    `select l.location_id, l.address_id
                    FROM rpm_client_location l WHERE l.client_id = ${params.client_id}`);

register_route(app,
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

const server = app.listen(5000, function () {
    const host = server.address().address;
    const port = server.address().port;
    console.log('client-mgt backend listening at http://%s:%s', host, port);
});

