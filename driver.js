var pg = require('pg');
var fs = require('fs');
var path = require('path');
var async = require('async-arrays');
var pg = require('pg');

var pool = new pg.Pool();

var Adapter = function(options){
    this.options = options || {};
    this.client = options.client || (options.pool && pool) || new pg.Client();
    this.connect(()=>{});
}

var renderUpsert = function(table, fields, pk, object, cb){
    var values = [];
    fields.forEach((field)=>{
        values.push(object[field])
    });
    //everything needs to be rendered as symbols, so pg escaping can work
    var symbols = values.map((item, index)=> '$'+(index+1) );
    var commas = ', ';
    var query = 'INSERT INTO '+table+'('+fields.join(commas)+') '+
                'VALUES ('+symbols.join(commas)+') '+
                'ON CONFLICT('+pk+') '+
                'DO UPDATE SET '+ fields.map(
                    (name, index)=> name+' = '+symbols[index]
                ).join(commas);
    if(cb) cb(null, query, values);
}

var sqlTypeFromJavascriptType = function(name, value, pk){
    var type = typeof value;
    if(type === 'object' && Array.isArray(value)) type = 'array';
    if(type === 'object' && value instanceof Date) type = 'datetime';
    if(type === 'number' && !isNaN(value)){
        // check if it is integer
        if( Number.isInteger(value) ) type = 'integer';
        else type = 'float';
    }
    var typeCreate;
    switch(type){
        case 'string' : typeCreate = 'VARCHAR (255)'; break;
        case 'integer' : typeCreate = 'INTEGER'; break;
        case 'bigint' : typeCreate = 'BIGINT'; break;
        case 'object' : typeCreate = 'JSON'; break;
        case 'float' : typeCreate = 'FLOAT (8)'; break;
        //todo: handle arrays + arrays of objects as FKs
    }
    if(!typeCreate) throw new Error('Unrecognized Type: '+type);
    if(name === pk ) return name + ' ' + typeCreate+ ' PRIMARY KEY';
    return name + ' ' + typeCreate+ ' NOT NULL';
}

var renderCreate = function(name, object, pk, callback){
    var fields = Object.keys(object);
    var creates = [];
    fields.forEach((field)=>{
        creates.push(sqlTypeFromJavascriptType(field, object[field], pk))
    });
    var sql = 'CREATE TABLE '+name+'('+creates.join(', ')+')';
    if(callback) setTimeout(()=>{
        callback(null, sql);
    }, 0);
}

Adapter.prototype.connect = function(name, options, handler, cb){
    if(!this.connection) this.connection = this.client.connect();
    this.connection.then((engine)=>{
        this.engine = engine || this.client;
        if(cb) cb(null)
    }).catch((err)=>{
        if(cb) cb(err)
    })
}

Adapter.prototype.load = function(name, options, handler, cb){
    var query = "SELECT * from "+name;
    //todo: handle query option
    this.connection.then(()=>{
        this.engine.query(query, (err, res)=>{
            if(res && res.rows){
                var result = res.rows;
                result.forEach((item)=> handler(item));
                if(cb) cb(null, result);
            }else{
                if(cb) cb( new Error('could not select data'));
            }
        });
    });
}

Adapter.prototype.cleanup = function(all){
    try{
        if(this.engine.release) this.engine.release(true);
        if(this.engine.end) this.engine.end();
        if(this.client.release) this.client.release(true);
        if(this.client.end) this.client.end();
    }catch(ex){}
}

Adapter.prototype.exists = function(name, options, cb){
    //todo: optional existence record
    //todo: optimized mode short circuit (errs on nonexistence )
    var query = "SELECT EXISTS ("+
                "SELECT 1 "+
                "FROM pg_tables "+
                "WHERE schemaname = '"+(this.schema || 'public')+"' "+
                "AND tablename = '"+name+"' "+
                ")";
    this.connection.then(()=>{
        this.engine.query(query, (err, res)=>{
            if(res && res.rows){
                var result = res.rows[0];
                if(!result.exists){
                    if(!options.object) return cb(new Error(
                        'Table does not exist, and an example object is needed to generate one!'
                    ));
                    renderCreate(name, options.object, (options.primaryKey || 'id'), (err, createQuery, values)=>{
                        this.engine.query(createQuery, values, (createErr, createRes)=>{
                            if(createErr) return cb(createErr);
                            cb(null, {created: true});
                        });
                    });
                }else{
                    cb();
                }
            }else{
                throw Error()
            }
        });
    });
}

Adapter.prototype.loadCollection = function(collection, name, options, cb){
    this.load(name, options, function(item){
      collection.index[item[collection.primaryKey]] = item;
    }, cb);
}

Adapter.prototype.saveCollection = function(collection, name, options, cb){
    var lcv = 0;
    var list;
    this.save(name, options, collection, function(){
        if(!list) list = Object.keys(collection.index);
        return list.length?collection.index[list.shift()]:null;
    }, cb);
}

Adapter.prototype.save = function(name, options, collection, next, cb){
    //loop through batches and save a chunk of ids as
    var writtenOne = false;
    var writeChain = ()=>{
        //todo: make chains batchable to a requestable size
        var item = next();
        if(item){
            var fields = Object.keys(item);
            renderUpsert(name, fields, collection.primaryKey, item, (err, query, values)=>{
                this.engine.query(query, values, (writeErr)=>{
                    if(!writtenOne) writtenOne = true;
                    if(writeErr) return cb(writeErr);
                    writeChain();
                })
            });
        }else{
            cb();
        }
    }
    this.exists(name, {}, ()=>{
        writeChain();
    });
}

Adapter.prototype.query = function(q, cb){
    //allows symbolic saving to be executed remotely (instead of as a set)

}

var term;

module.exports = {
  Adapter : Adapter
}
