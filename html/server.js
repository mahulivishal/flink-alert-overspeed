const express = require('express');
const app = express();

app.get('/overspeed-alert/sse/d_01', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    let requestData = '';
    // Set encoding to utf-8 to ensure that data is received as a string
    req.setEncoding('utf-8');

    // Listen for 'data' event to read request data
    req.on('data', (chunk) => {
            res.write(chunk)
            res.writeHead(200, {'Content-Type': 'text/plain'});

        });

    req.on('end', () => {
        // Now requestData contains the entire request body as a string
        console.log('Request data:', requestData);
    
        // Send response
        res.writeHead(200, {'Content-Type': 'text/plain'});
        res.end('Received request data: ' + requestData);
    });
});

app.listen(3000, () => {
    console.log('Server running on http://localhost:3000');
});