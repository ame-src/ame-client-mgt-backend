var express = require('express');
var app = express();

// config for your database
var config = {
    user: 'sa',
    password: 'topdog',
    server: 'database.amemusic.com', 
    database: 'AmeMaster',
    trustServerCertificate: true
};

function register_client_route(app, url, sql_builder){

    app.get(
        url,
        function(req, res) {

            var sql = require("mssql");

            try{

                if(!req.params){
                    throw new Error("no parameters, please provide client-id at least");
                }

                if(req.params.client_id == ""){
                    throw new Error("no client-id paramter provided");
                }

                
                var client_id = parseInt(req.params.client_id);
                if(isNaN(client_id) || isNaN(parseFloat(client_id))){
                    throw new Error("client id not a number");
                }
            }
            catch(e)
            {
                res.status(400).json({ message: e.message})
                return;
            }

            // connect to your database
            sql.connect(config, function (err) {
                
                if (err) {
                    res.status(500).json({message: err.message});
                    return;
                }

                // create Request object
                var request = new sql.Request();
                
                // query to the database and get the records
                request.query(sql_builder(client_id), 
                    function (err, results) {
                    
                        if (err) {
                            res.status(500).json({message: err.message});
                            return;
                        }

                        // send records as a response
                        res.send(results.recordset);
                });
            });
        }
    );
}

register_client_route(app,
                '/addresses/:client_id',
                client_id => 
                    `select address_id, company_id, company_type, company_name, branch_type, Branch,
                        Member, Attention, address_1, address_2, address_3, 
                        City, State, Country, Zip, cast(case when notes is not null then notes else '' end as varchar(MAX)) as Notes, 
                        case when ol.seq_num is not null then ol.seq_num else 4 end as sort_key  
                        from rpm_client_address ca 
                        left outer join rpm_option_list ol on ol.application = 'AMECLIENTMGT' and ol.option_type = 'BRANCH_TYPE' and ol.option_value = ca.branch_type 
                        WHERE company_type = 'C' AND company_id = ${client_id} 
                        order by sort_key, address_1`);

register_client_route(app,
                '/locations/:client_id',
                client_id => 
                    `select l.location_id, l.address_id, l.bill_to_address_id, l.ship_to_address_id, l.bill_to_policy, 
                    l.ship_to_policy, l.monthly_service_fee, l.months_per_bill_period, l.bill_ahead_days,
                    l.ascap_rate_plan, l.service_start_date, l.qc_call_date, l.season_start, l.season_end,
                    l.zone_id, l.system_grace_period, l.prev_bill_through_date, 
                    cast(case when l.Notes is not null then l.Notes else '' end as varchar(MAX)) as Notes, 
                    l.update_method, l.disc_threshold, l.update_method_reason, l.location_type, 
                    l.location_status, b.billed_from_date, b.billed_through_date, b.paid_through_date,
                    l.charge_template_id 
                    FROM rpm_client_location l 
                    INNER JOIN qry_location_billing_dates b ON l.location_id = b.location_id WHERE l.client_id = ${client_id}`);

var server = app.listen(5000, function () {
    console.log('Server is running..');
});