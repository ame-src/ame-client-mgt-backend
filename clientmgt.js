var express = require('express');
var app = express();

app.get('/locations/:client_id', function (req, res) {
   
    var sql = require("mssql");

    // config for your database
    var config = {
        user: 'sa',
        password: 'topdog',
        server: 'database.amemusic.com', 
        database: 'AmeMaster',
        trustServerCertificate: true
    };

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

    try {

        // connect to your database
        sql.connect(config, function (err) {
        
                if (err) console.log(err);

                // create Request object
                var request = new sql.Request();
                
                // query to the database and get the records
                request.query(`select l.location_id, l.address_id, l.bill_to_address_id, l.ship_to_address_id, l.bill_to_policy, 
                l.ship_to_policy, l.monthly_service_fee, l.months_per_bill_period, l.bill_ahead_days,
                l.ascap_rate_plan, l.service_start_date, l.qc_call_date, l.season_start, l.season_end,
                l.zone_id, l.system_grace_period, l.prev_bill_through_date, 
                cast(case when l.Notes is not null then l.Notes else '' end as varchar(MAX)) as Notes, 
                l.update_method, l.disc_threshold, l.update_method_reason, l.location_type, 
                l.location_status, b.billed_from_date, b.billed_through_date, b.paid_through_date,
                l.charge_template_id 
                FROM rpm_client_location l INNER JOIN qry_location_billing_dates b ON l.location_id = b.location_id WHERE l.client_id = ${client_id}`, 
                    function (err, recordset) {
                    
                        if (err) console.log(err)

                        // send records as a response
                        res.send(recordset);
                });
    });
    }
    catch(e){
        res.status(500).json({ message: e.message})
        return;
    }
});

var server = app.listen(5000, function () {
    console.log('Server is running..');
});