const net = require('net');
const Parser = require('teltonika-parser-ex');
const binutils = require('binutils64');
const { Client } = require('pg');
const host = process.env.HOST
const port = process.env.PORT

let server = net.createServer((c) => {
    //console.log(c);
    const clientIp = c.remoteAddress;
    const clientPort = c.remotePort;
    try {
        console.log(`client connected from ${clientIp} : ${clientPort}`);
    } catch (err) {
        console.log(err)
    }

    let imei;
    let imei_id;

    c.on('end', () => {
        try {
            console.log(`client ${clientIp} : ${clientPort} disconnected`);
        } catch (err) {
            console.log(err)
        }
    });

    c.on('error', (err) => {
        console.log('err', err)
    })

    c.on('data', async (data) => {

        try {
            let buffer = data;
            console.log(buffer.toString());

            let parser = new Parser(buffer);

            if (parser.isImei) {
                imei = buffer.toString('utf-8').slice(2, 17);
                //console.log('imei:' , imei)
                imei_id = await validateImei(imei);
                //console.log(imei_id);
                if (imei_id != 0) {
                    c.write(Buffer.alloc(1, 1)); // send ACK for IMEI
                }
            } else if (buffer.toString().length < 10) {
                console.log('sync signal');
            } else {
                let avl = parser.getAvl();
                
                //console.log(avl.records[0].ioElements);
                
                //console.log(avl.records[0].ioElements[0]);
                
                const gpsData = avl.records.map(({ gps, timestamp, event_id, priority, ioElements }) => {
                    return { gps, timestamp, event_id, priority, ioElements }
                })
                addCoords(gpsData, imei_id, imei);

                let writer = new binutils.BinaryWriter();
                try {
                    writer.WriteInt32(avl.number_of_data);
                    let response = writer.ByteBuffer;

                    c.write(response); // send ACK for AVL DATA
                    
                } catch(err){
                    console.log('write data error');
                    console.log('err3: ', err);
                }

               
            }
        } catch (err) {

            console.log(err)

        }

    });
});

server.listen(port, host, () => {
    console.log(`Server started on ${host}:${port}`);
});

async function addCoords(gpsData, imei_id, imei) {


    let client = new Client({
        host: process.env.PGHOST,
        port: process.env.PGPORT,
        user: process.env.PGUSER,
        database: process.env.PGDB,
        password: process.env.PGPASS,
        ssl: false,
    })

	try{
    await client.connect();
    for (let ii = 0; ii < gpsData.length; ii++) {
        const data = gpsData[ii];
        //console.log('data: ', data);
        //console.log(data.gps);
        //console.log('io:', data.ioElements);
        //const timestamp = Date(data.timestamp).getTime();
        const timestamp = Math.floor(new Date(data.timestamp).getTime() / 1000);
        const query = 'insert into trackers.positions (imei_id, imei, altitude, speed, ' +
            ' angle, satellites, coords, timestamp, event_id, priority, ioelements ) ' +
            ' VALUES ' +
            '(' + imei_id + ",'" + imei + "'," +
            data.gps.altitude + ',' + data.gps.speed + ',' + data.gps.angle + ',' + data.gps.satellites + ',' +
            "st_geomfromtext('" + "POINT(" + data.gps.longitude + ' ' + data.gps.latitude + ")')" + ',' +
            timestamp + ',' + data.event_id + ',' + data.priority + ',' 
            + " '" +  JSON.stringify(data.ioElements) + "' " +   ')';

        console.log(query);
        const res = await client.query(query);
    }

    await client.end();
    console.log('Rows added: ', gpsData.length);
    } catch(err){
        console.log("err1: ", err);
        
        try {
            await client.end();
        } catch(err){
            console.log("err2:", err);
        }
    }
}

async function validateImei(imei) {
    
    
    let imei_id = 0;
    let client = new Client({
        host: process.env.PGHOST,
        port: process.env.PGPORT,
        user: process.env.PGUSER,
        database: process.env.PGDB,
        password: process.env.PGPASS,
        ssl: false,
    })
    
    try{
    
    await client.connect();
    const query = "select * from trackers.imeis where imei = '" + imei + "'";
    const res = await client.query(query);
    await client.end();
    if (res.rows.length > 0) {
        imei_id = res.rows[0].id;
    }

    return imei_id
    } catch(err){
        console.log('imei err', err);
    
        try{
            await client.end();
        } catch(err){
        console.log('imei catch', err);
        }
    }
}