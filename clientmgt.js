var express = require('express');
var sql = require("mssql");
var app = express();

// config for your database
var config = {
    user: 'sa',
    password: 'topdog',
    server: 'database.amemusic.com', 
    database: 'AmeMaster',
    trustServerCertificate: true
};

const app_pool = new sql.ConnectionPool(config);

function register_route(app, url, params, sql_builder){

    app.get(
        `/${url}/:${Object.keys(params).join(",")}`,
        async function(req, res) {

            try{

                if(Object.keys(params).length > 0){

                    if(!req.params){
                        throw new Error(`no parameters, please provide ${req.params[0]} at least`);
                    }

                    for(var param in params){

                        if(req.params[param] == ""){
                            throw new Error(`no ${param} parameter provided`);
                        }

                        if(params[param] == "Int"){
                            var temp = parseInt(req.params[param]);
                            if(isNaN(temp) || isNaN(parseFloat(temp))){
                                throw new Error(`${param} id not a number`);
                            }   
                        }
                    }
                }
            }
            catch(e)
            {
                res.status(400).json({ message: e.message})
                return;
            }

            try {            
                // query to the database and get the records
                let rows = await app.locals.db.query(sql_builder(req.params)); 
                // send records as a response
                res.send(rows.recordset);
            }
            catch(err){
                res.status(500).json({message: err.message});
            }
        }
    );
}

register_route(app,
                "addresses",
                {"client_id":"Int"},
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
"address",
{"address_id":"Int"},
params => 
    `select address_id, company_id, company_type, company_name, branch_type, Branch,
        Member, Attention, address_1, address_2, address_3, 
        City, State, Country, Zip, cast(case when notes is not null then notes else '' end as varchar(MAX)) as Notes, 
        case when ol.seq_num is not null then ol.seq_num else 4 end as sort_key  
        from rpm_client_address ca 
        left outer join rpm_option_list ol on ol.application = 'AMECLIENTMGT' and ol.option_type = 'BRANCH_TYPE' and ol.option_value = ca.branch_type 
        WHERE company_type = 'C' AND address_id = ${params.address_id} 
        order by sort_key, address_1`);

register_route(app,
                "locations",
                {"client_id":"Int"},
                params => 
                    `select l.location_id, l.address_id
                    FROM rpm_client_location l WHERE l.client_id = ${params.client_id}`);

register_route(app,
    "location",
    {"location_id":"Int"},
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

//connect the pool and start the web server when done
app_pool.connect().then(function(pool) {
    app.locals.db = pool;
    const server = app.listen(5000, function () {
      const host = server.address().address
      const port = server.address().port
      console.log('client-mgt backend listening at http://%s:%s', host, port)
    })
  }).catch(function(err) {
    console.error('Error creating connection pool', err)
  });