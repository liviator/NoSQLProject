const express = require('express');
const routeur = express.Router();
const obj = require('../controller/object');

routeur.post("/register",obj.register);


module.exports = routeur