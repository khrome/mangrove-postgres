var should = require("chai").should();
var Indexed = require('indexed-set');
var MangrovePostgres = require('../driver');
var pg = require('pg');
var pool = new pg.Pool();

var data = {
    test_table : [
        {
            id : 234,
            name: 'blah',
            v: 2.756
        }
    ],
    test_write_table : [
        {
            id : 234,
            name: 'blah',
            v: 2.756
        },
        {
            id : 456,
            name: 'baz',
            v: 3.4895
        },
        {
            id : 139,
            name: 'bar',
            v: 2.48
        },
        {
            id : 493,
            name: 'foo',
            v: 2.0863
        }
    ],
}



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

    describe('.saveCollection(<collection>, <name>, <options>, <callback>)', function(){
        it('saves 3 random objects', function(done){
            pool.connect().then((client)=>{
                var adapter = new MangrovePostgres.Adapter({});
                adapter.exists('test_write_table', {
                    pool:true,
                    object : data['test_write_table'][0]
                }, (err)=>{
                    should.not.exist(err);
                    var loaded = new Indexed.Collection(data['test_write_table'], 'id');
                    var byId = (a, b) => { return a.id < b.id?-1:1 };
                    adapter.saveCollection(loaded, 'test_write_table', {}, (saveErr)=>{
                        should.not.exist(saveErr);
                        client.query('SELECT * FROM test_write_table', (reselectErr, res)=>{
                            client.query('DROP TABLE test_write_table', (dropErr)=>{
                                should.not.exist(dropErr);
                                data['test_write_table'].length.should.equal(res.rows.length);
                                var testData = data['test_write_table'].sort(byId);
                                var returnData = res.rows.sort(byId);
                                testData.should.deep.equal(returnData);
                                client.release(true);
                                adapter.cleanup();
                                done();
                            });
                        });
                    });
                });
            });
        });
    });
});
