const net = require("net");
const Parser = require("teltonika-parser-ex");
const binutils = require("binutils64");
const {
    Client,
    Pool
} = require("pg");
const host = process.env.HOST;
const port = process.env.PORT;

var tcpClients = [];
var imeis =[];
let server = net.createServer((c) => {
    //console.log(c);
    const clientIp = c.remoteAddress;
    const clientPort = c.remotePort;
    try {
		
        console.log(`client connected from ${clientIp} : ${clientPort}`);
    } catch (err) {
        console.log('err: ', err);
    }

    let imei;
    let imei_id;

    c.on("end", () => {
        try {
			
			const idx = imeis.indexOf(imei);
			tcpClients[idx] = 0;
			const totalClient = tcpClients.reduce((partialSum, a) => partialSum + a, 0);
            console.log(`client ${clientIp} : ${clientPort} disconnected  -  clients: ${totalClient}` );
			
        } catch (err) {
            console.log('err0: ', err);
        }
    });

    c.on("error", (err) => {
        console.log("err", err);
    });

    c.on("data", async(data) => {
        try {
            let buffer = data;
            //console.log(buffer.toString());

            let parser = new Parser(buffer);

            if (parser.isImei) {
                try {
                    imei = buffer.toString("utf-8").slice(2, 17);
                    //console.log('imei:' , imei)
                    imei_id = await validateImei(imei);
                    //console.log(imei_id);
                    if (imei_id != 0) {
						
						if(!imeis.includes(imei)){
							imeis.push(imei);
							tcpClients.push(1);
							
						} else{
							const idx = imeis.indexOf(imei);
							tcpClients[idx] = 1;
						}
                        c.write(Buffer.alloc(1, 1)); // send ACK for IMEI
                    }
                } catch (err) {
                    console.log('imei error: ', err);
                }
            } else if (buffer.toString().length < 10) {
                console.log("sync signal");
            } else {
                let avl = parser.getAvl();

                //console.log(avl.records[0].ioElements);

                //console.log(avl.records[0].ioElements[0]);

                const gpsData = avl.records.map(
                        ({
                            gps,
                            timestamp,
                            event_id,
                            priority,
                            ioElements
                        }) => {
                        return {
                            gps,
                            timestamp,
                            event_id,
                            priority,
                            ioElements
                        };
                    });
                let statFlag;

                statFlag = await addCoords(gpsData, imei_id, imei);

                let writer = new binutils.BinaryWriter();
				
				const totalClient = tcpClients.reduce((partialSum, a) => partialSum + a, 0);
                console.log(imei + ' - ' + statFlag + ' - clients: ' + totalClient );
                try {
                    if (statFlag == true) {
                        writer.WriteInt32(avl.number_of_data);
                        let response = writer.ByteBuffer;

                        c.write(response); // send ACK for AVL DATA
                        //console.log("confirmation: ", writer.ByteBuffer);
                    }
                } catch (err) {
                    console.log("write data error");
                    console.log("err3: ", err);
                }
            }
        } catch (err) {
            console.log(err);
        }
    });
});

server.listen(port, host, () => {
    console.log(`Server started on ${host}:${port}`);
});

async function addCoords(gpsData, imei_id, imei) {

    //console.log('imei: ', imei);
    let data;
    let timestamp;
    let jj = 0;
    let ignstat;
    let mvntstat;
    let query;
    let statFlag = true;
    try {

        if (imei_id == 0) {
            console.log('not valid imei: ', imei_id);
            statFlag = 0;
            return statFlag;
        }
        for (let ii = 0; ii < gpsData.length; ii++) {

            timestamp = Math.floor(new Date(gpsData[ii].timestamp).getTime() / 1000);
            if (isNaN(timestamp)) {
                console.log('nan detected');
                statFlag = 0;
                return statFlag;
            }
        }

        let client = new Client({
            host: process.env.PGHOST,
            port: process.env.PGPORT,
            user: process.env.PGUSER,
            database: process.env.PGDB,
            password: process.env.PGPASS,
            ssl: false,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 30000,
            statement_timeout: 50000
        });

        await client.connect();

        client.on('error', (err) => {
            console.error('client error: ', err.stack)
        })

        for (let ii = 0; ii < gpsData.length; ii++) {
            data = gpsData[ii];

            let ign = data.ioElements.find((o) => o.id === 239);
            let mvt = data.ioElements.find((o) => o.id === 240);
            //console.log(ign);
            //console.log(mvt);

            ignstat = null;
            if (ign != undefined) {
                ignstat = ign.value;
            }

            mvntstat = null;
            if (mvt != undefined) {
                mvntstat = mvt.value;
            }

            timestamp = Math.floor(new Date(data.timestamp).getTime() / 1000);

            if (isNaN(timestamp)) {
                console.log('nan detected');
                statFlag = 0;
                try {
                    await client.end();
                } catch (err) {
                    console.log('err: ', err);
                }
                return statFlag;
            }
            //console.log('timestamp2:, ', timestamp2);
            //console.log('timestamp:, ', timestamp);

            query =
                "insert into trackers.positions (imei_id, imei, altitude, speed, " +
                " angle, satellites, coords, timestamp, event_id, priority, ioelements, engine, ismoving ) " +
                " VALUES " +
                "(" +
                imei_id +
                ",'" +
                imei +
                "'," +
                data.gps.altitude +
                "," +
                data.gps.speed +
                "," +
                data.gps.angle +
                "," +
                data.gps.satellites +
                "," +
                "st_geomfromtext('" +
                "POINT(" +
                data.gps.longitude +
                " " +
                data.gps.latitude +
                ")')" +
                "," +
                timestamp +
                "," +
                data.event_id +
                "," +
                data.priority +
                "," +
                " '" +
                JSON.stringify(data.ioElements) +
                "' ," +
                ignstat +
                "," +
                mvntstat +
                ")";

            //console.log(query);

            try {
                const res = await client.query(query);
            } catch (err) {
                console.log('qerror:', err);
                console.log(query);

                statFlag = false;
                return statFlag;
            }
            jj++;
        }

        const query2 =
            "update trackers.location " +
            " set " +
            "altitude = " +
            data.gps.altitude +
            ", speed= " +
            data.gps.speed +
            " ,angle=" +
            data.gps.angle +
            ",satellites=" +
            data.gps.satellites +
            ", coords=" +
            "st_geomfromtext('" +
            "POINT(" +
            data.gps.longitude +
            " " +
            data.gps.latitude +
            ")')" +
            ", timestamp=" +
            timestamp +
            ", event_id=" +
            data.event_id +
            " ,priority=" +
            data.priority +
            ", ioelements=" +
            " '" +
            JSON.stringify(data.ioElements) +
            "' " +
            ", engine=" +
            ignstat +
            ", ismoving=" +
            mvntstat +
            " where imei = '" +
            imei +
            "'";

        //console.log(query2);
        const res2 = await client.query(query2);

        try {
            await client.end();

        } catch (err4) {
            console.log('err4: ', err);
            await client.end();
        }

        return statFlag;
    } catch (err) {
        console.log("err1: ", err);
        statFlag = false;
        try {
            await client.end();
        } catch (err) {
            console.log("err2:", err);

        }
        return statFlag;
    }
    return statFlag;
}

async function validateImei(imei) {
    //console.log('imei: ', imei);
    let imei_id = 0;
    if (imei.match(/[^0-9]{1,}/) != null) {
        console.log('not a valid imei: ', imei);
        return imei_id
    }
    try {

        let client = new Client({
            host: process.env.PGHOST,
            port: process.env.PGPORT,
            user: process.env.PGUSER,
            database: process.env.PGDB,
            password: process.env.PGPASS,
            ssl: false,
            max: 20,
            idleTimeoutMillis: 3000,
            connectionTimeoutMillis: 3000,
            statement_timeout: 5000
        });

        await client.connect();
        client.on('error', (err) => {
            console.error('client error: ', err.stack)
        });
        const query = "select * from trackers.imeis where imei = '" + imei + "'";
        //console.log(query);
        const res = await client.query(query);
        await client.end();
        if (res.rows.length > 0) {
            imei_id = res.rows[0].id;
        }

    } catch (err) {
        console.log("imei err", err);

        try {
            await client.end();
        } catch (err) {
            console.log("imei catch", err);
        }
    }
    return imei_id;
}
