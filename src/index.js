const bodyParser = require('body-parser')
const express = require('express')
const obj = require('./route/object')

const app = express()
const port = 3000
const cors = require("cors");
app.use(cors());
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }));
app.use("/obj",obj)



app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
  })