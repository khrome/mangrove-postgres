var should = require("chai").should();
var Indexed = require('indexed-set');
var MangrovePostgres = require('../driver');
var pg = require('pg');
var pool = new pg.Pool();

var data = require('./data.json');

describe('Mangrove PostgreSQL Adapter', function(){

    describe('.exists(<collection_name>, <options>, <callback>)', function(){

        it('tests existence and fails, with no object', function(done){
            pool.connect().then((client)=>{
                var adapter = new MangrovePostgres.Adapter({pool:true});
                adapter.exists('test_table', {}, (err)=>{
                    should.exist(err);
                    adapter.cleanup();
                    client.release(true);
                    done();
                });
            });
        });

        it('tests existence and creates a table and succeeds, with an object', function(done){
            pool.connect().then((client)=>{
                var adapter = new MangrovePostgres.Adapter({});
                adapter.exists('test_table', {
                    pool:true,
                    object : data['test_table'][0]
                }, (err)=>{
                    should.not.exist(err);
                    client.query('DROP TABLE test_table', (dropErr)=>{
                        should.not.exist(dropErr);
                        client.release(true);
                        adapter.cleanup();
                        done();
                    });
                });
            });
        });

        /*it('loads a saved table', function(done){

        });*/

    });

    var byId = (a, b)=>{ return a.id < b.id?-1:1 };

    var saveObjects = function(adapter, table_name, obs, cb){
        var primaryKey = 'id';
        pool.connect().then((client)=>{
            adapter.exists(table_name, {
                pool : true,
                primaryKey: primaryKey,
                object : obs[0]
            }, (err)=>{
                should.not.exist(err);
                var loaded = new Indexed.Collection(obs, primaryKey);
                adapter.saveCollection(loaded, table_name, {}, (saveErr)=>{
                    should.not.exist(saveErr);
                    client.query('SELECT * FROM '+table_name, (rErr, res)=>{
                        should.not.exist(rErr);
                        obs.length.should.equal(res.rows.length);
                        var testData = obs.sort(byId);
                        var returnData = res.rows.sort(byId);
                        testData.should.deep.equal(returnData);
                        cb(null, client);
                    });
                });
            });
        });
    }

    describe('.saveCollection(<collection>, <name>, <options>, <callback>)', function(){

        it('saves some random objects', function(done){
            var adapter = new MangrovePostgres.Adapter({});
            var table_name = 'test_write_table';
            saveObjects(adapter, table_name, data[table_name], (err, client)=>{
                client.query('DROP TABLE '+table_name, (dropErr)=>{
                    client.release(true);
                    adapter.cleanup();
                    done();
                });
            });

        });

    });

    describe('.loadCollection(<collection>, <name>, <options>, <callback>)', function(){

        it('can load some data we save immediately before', function(done){
            var adapter = new MangrovePostgres.Adapter({});
            var table_name = 'test_write_table';
            var collection = new Indexed.Collection([], 'id');
            saveObjects(adapter, table_name, data[table_name], (err, client)=>{
                adapter.loadCollection(collection, table_name, {}, (loadErr)=>{
                    should.not.exist(loadErr);
                    client.query('DROP TABLE '+table_name, (dropErr)=>{
                        (
                            new Indexed.Set(collection)
                        ).toArray().sort(byId).should.deep.equal(
                            data[table_name].sort(byId)
                        );
                        should.not.exist(dropErr);
                        client.release(true);
                        adapter.cleanup();
                        done();
                    });
                });
            });

        });

    });

});
