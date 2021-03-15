const mysql = require('mysql')
const pool = require('../model/db')

exports.register = (req, res, next) => {


    sql='INSERT INTO jsonfile (Id,Event_type, Date_creation, Date_ajout, Version_file, Graph_id, Nature, Object_name, Object_path) VALUES (?,?,?,?,?,?,?,?,?)'
    var insert = [req.body.Id,req.body.Event_type,req.body.Date_creation,req.body.Date_ajout,req.body.Version_file,req.body.Graph_id,req.body.Nature,req.body.Object_name,req.body.Object_path]
    sql = mysql.format(sql,insert);
    pool.query(sql,function(error,results,fields){
    if(error) throw error;              
            res.status(201).json({ message:"Test reussi" })
    })
    
    
};