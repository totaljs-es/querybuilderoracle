// Total.js Module: Oracle integrator
const oracledb = require('oracledb');
const CANSTATS = global.F ? (global.F.stats && global.F.stats.performance && global.F.stats.performance.dbrm != null) : false;
const REG_ORACLE_ESCAPE = /'/g;
const REG_LANGUAGE = /[a-z0-9]+§/gi;
const REG_WRITE = /(INSERT|UPDATE|DELETE|DROP)\s/i;
const REG_COL_TEST = /"|\s|:|\./;
const REG_UPDATING_CHARS = /^[-+*/><!=#]/;
const LOGGER = ' -- ORACLE -->';

const POOLS = {};
var FieldsCache = {};

function exec(client, filter, callback, done, errorhandling) {
    var cmd;

    if (filter.exec === 'list') {
        try {
            cmd = makesql(filter);
        } catch (e) {
            done();
            callback(e);
            return;
        }

        if (filter.debug)
            console.log(LOGGER, cmd.query, cmd.params);

        client.execute(cmd.query, cmd.params, { outFormat: oracledb.OBJECT, autoCommit: true }, function(err, response) {
            if (err) {
                done();
                errorhandling && errorhandling(err, cmd);
                callback(err);
                return;
            }

            cmd = makesql(filter, 'count');

            if (filter.debug)
                console.log(LOGGER, cmd.query, cmd.params);

            client.execute(cmd.query, cmd.params, { outFormat: oracledb.OBJECT, autoCommit: true }, function(err, counter) {
                done();
                err && errorhandling && errorhandling(err, cmd);
                callback(err, err ? null : { items: response.rows, count: +counter.rows[0].COUNT });
            });
        });
        return;
    }

    try {
        cmd = makesql(filter);
    } catch (e) {
        done();
        callback(e);
        return;
    }

    if (filter.debug)
        console.log(LOGGER, cmd.query, cmd.params);

    client.execute(cmd.query, cmd.params, { outFormat: oracledb.OBJECT, autoCommit: true }, function(err, response) {
        done();

        if (err) {
            errorhandling && errorhandling(err, cmd);
            callback(err);
            return;
        }

        var output;

        switch (filter.exec) {
            case 'remove':
                output = response.rowsAffected;
                callback(null, output);
                break;
            case 'check':
                output = response.rows && response.rows.length > 0;
                callback(null, output);
                break;
            case 'count':
                output = response.rows && response.rows[0] ? response.rows[0].COUNT : null;
                callback(null, output);
                break;
            case 'scalar':
                output = filter.scalar.type === 'group' ? response.rows : (response.rows[0] ? response.rows[0].VALUE : null);
                callback(null, output);
                break;
            case 'insert':
                output = response.rowsAffected;
                callback(null, output);
                break;
            case 'update':
                output = response.rowsAffected;
                callback(null, output);
                break;
            default:
                output = response.rows;
                callback(null, output);
                break;
        }
    });
}

function oracle_where(where, opt, filter, operator, params) {
    for (let item of filter) {
        let name = '';
        if (item.name) {
            let key = 'where_' + (opt.language || '') + '_' + item.name;
            name = FieldsCache[key];
            if (!name) {
                name = item.name;
                if (name[name.length - 1] === '§')
                    name = replacelanguage(item.name, opt.language, true);
                else
                    name = REG_COL_TEST.test(item.name) ? item.name : ('"' + item.name + '"');
                FieldsCache[key] = name;
            }
        }

        switch (item.type) {
            case 'or': {
                let tmp = [];
                oracle_where(tmp, opt, item.value, 'OR', params);
                if (tmp.length) {
                    if (where.length) where.push(operator);
                    where.push('(' + tmp.join(' ') + ')');
                }
                break;
            }
            case 'in':
            case 'notin': {
                if (where.length) where.push(operator);
                let arr = Array.isArray(item.value) ? item.value : [item.value];
                let binds = arr.map(v => {
                    params.push(v);
                    return ':' + params.length;
                });
                where.push(name + (item.type === 'in' ? ' IN ' : ' NOT IN ') + '(' + binds.join(',') + ')');
                break;
            }
            case 'between': {
                if (where.length) where.push(operator);
                params.push(item.a);
                params.push(item.b);
                where.push(name + ' BETWEEN :' + (params.length - 1) + ' AND :' + params.length);
                break;
            }
            case 'search': {
                if (where.length) where.push(operator);
                params.push('%' + item.value + '%');
                where.push('LOWER(' + name + ') LIKE LOWER(:' + params.length + ')');
                break;
            }
            case 'empty': {
                if (where.length) where.push(operator);
                where.push('(' + name + ' IS NULL OR LENGTH(' + name + ')=0)');
                break;
            }
            case 'where': {
                if (where.length) where.push(operator);
                if (item.value == null)
                    where.push(name + (item.comparer === '=' ? ' IS NULL' : ' IS NOT NULL'));
                else {
                    params.push(item.value);
                    where.push(name + (item.comparer || '=') + ' :' + params.length);
                }
                break;
            }
            case 'query': {
                if (where.length) where.push(operator);
                where.push('(' + item.value + ')');
                break;
            }
            case 'month':
            case 'year':
            case 'day':
            case 'hour':
            case 'minute': {
                if (where.length) where.push(operator);
                params.push(item.value);
                where.push('EXTRACT(' + item.type.toUpperCase() + ' FROM ' + name + ') = :' + params.length);
                break;
            }
        }
    }
}

function oracle_insertupdate(filter, insert) {
    var query = [];
    var fields = insert ? [] : null;
    var params = [];

    for (var key in filter.payload) {
        var val = filter.payload[key];
        if (val === undefined)
            continue;

        var c = key[0];
        switch (c) {
            case '-':
            case '+':
            case '*':
            case '/':
                key = key.substring(1);
                params.push(val ? val : 0);
                if (insert) {
                    fields.push('"' + key + '"');
                    query.push(':' + params.length);
                } else {
                    query.push('"' + key + '" = "' + key + '" ' + c + ' :' + params.length);
                }
                break;
            case '>':
            case '<':
                key = key.substring(1);
                params.push(val ? val : 0);
                if (insert) {
                    fields.push('"' + key + '"');
                    query.push(':' + params.length);
                } else {
                    query.push('"' + key + '" = ' + (c === '>' ? 'GREATEST' : 'LEAST') + '("' + key + '", :' + params.length + ')');
                }
                break;
            case '!':
                key = key.substring(1);
                if (insert) {
                    fields.push('"' + key + '"');
                    query.push('0');
                } else {
                    query.push('"' + key + '" = CASE WHEN "' + key + '" = 1 THEN 0 ELSE 1 END');
                }
                break;
            case '=':
            case '#':
                key = key.substring(1);
                if (insert) {
                    if (c === '=') {
                        fields.push('"' + key + '"');
                        query.push(val);
                    }
                } else {
                    query.push('"' + key + '" = ' + val);
                }
                break;
            default:
                params.push(val);
                if (insert) {
                    fields.push('"' + key + '"');
                    query.push(':' + params.length);
                } else {
                    query.push('"' + key + '" = :' + params.length);
                }
                break;
        }
    }

    return { fields, query, params };
}

function replacelanguage(fields, language, noas) {
    return fields.replace(REG_LANGUAGE, function(val) {
        val = val.substring(0, val.length - 1);
        return '"' + val + (noas ? (language || '') + '"' : language ? (language + '" AS "' + val + '"') : '"');
    });
}

function makesql(opt, exec) {
    var query = '';
    var where = [];
    var model = {};
    var isread = false;
    var params;
    var returning;
    var tmp;

    if (!exec)
        exec = opt.exec;

    params = [];
    oracle_where(where, opt, opt.filter || [], 'AND', params);

    var language = opt.language || '';
    var fields;
    var sort;

    if (opt.fields) {
        let key = 'fields_' + language + '_' + opt.fields.join(',');
        fields = FieldsCache[key] || '';
        if (!fields) {
            for (let i = 0; i < opt.fields.length; i++) {
                let m = opt.fields[i];
                if (m[m.length - 1] === '§')
                    fields += (fields ? ',' : '') + replacelanguage(m, opt.language);
                else
                    fields += (fields ? ',' : '') + (REG_COL_TEST.test(m) ? m : ('"' + m + '"'));
            }
            FieldsCache[key] = fields;
        }
    }

    switch (exec) {
        case 'find':
        case 'read':
        case 'list':
            query = 'SELECT ' + (fields || '*') + ' FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
            isread = true;
            break;
        case 'count':
            opt.first = true;
            query = 'SELECT COUNT(1) AS COUNT FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
            isread = true;
            break;
        case 'insert':
            returning = opt.returning ? opt.returning.join(', ') : (opt.primarykey || '');
            tmp = oracle_insertupdate(opt, true);
            query = 'INSERT INTO ' + opt.table2 + ' (' + tmp.fields.join(',') + ') VALUES(' + tmp.query.join(',') + ')';
            params = tmp.params;
            break;
        case 'remove':
            query = 'DELETE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
            break;
        case 'update':
            tmp = oracle_insertupdate(opt);
            query = 'UPDATE ' + opt.table2 + ' SET ' + tmp.query.join(',') + (where.length ? (' WHERE ' + where.join(' ')) : '');
            params = tmp.params;
            break;
        case 'check':
            query = 'SELECT 1 AS COUNT FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ') + ' FETCH FIRST 1 ROWS ONLY') : '');
            isread = true;
            break;
        case 'drop':
            query = 'DROP TABLE ' + opt.table2;
            break;
        case 'truncate':
            query = 'TRUNCATE TABLE ' + opt.table2;
            break;
        case 'scalar':
            switch (opt.scalar.type) {
                case 'avg':
                case 'min':
                case 'sum':
                case 'max':
                case 'count': {
                    opt.first = true;
                    var val = opt.scalar.key === '*' ? '1' : opt.scalar.key;
                    query = 'SELECT ' + opt.scalar.type.toUpperCase() + '(' + val + ') AS VALUE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
                    break;
                }
                case 'group': {
                    query = 'SELECT ' + opt.scalar.key + ', ' + (opt.scalar.key2 ? 'SUM(' + opt.scalar.key2 + ')' : 'COUNT(1)') + ' AS VALUE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '') + ' GROUP BY ' + opt.scalar.key;
                    break;
                }
            }
            isread = true;
            break;
        case 'query':
            if (where.length) {
                let wherem = opt.query.match(/\{where\}/ig);
                let wherec = 'WHERE ' + where.join(' ');
                query = wherem ? opt.query.replace(wherem, wherec) : (opt.query + ' ' + wherec);
            } else {
                query = opt.query;
            }
            params = opt.params;
            isread = REG_WRITE.test(query) ? false : true;
            break;
    }

    if (exec === 'find' || exec === 'read' || exec === 'list' || exec === 'query' || exec === 'check') {
        if (opt.sort) {
            let key = 'sort_' + language + '_' + opt.sort.join(',');
            sort = FieldsCache[key] || '';
            if (!sort) {
                for (let i = 0; i < opt.sort.length; i++) {
                    let m = opt.sort[i];
                    let index = m.lastIndexOf('_');
                    let name = m.substring(0, index);
                    let value = (REG_COL_TEST.test(name) ? name : ('"' + name + '"')).replace(/§/, language);
                    sort += (sort ? ',' : '') + value + ' ' + (m.substring(index + 1).toLowerCase() === 'desc' ? 'DESC' : 'ASC');
                }
                FieldsCache[key] = sort;
            }
            query += ' ORDER BY ' + sort;
        }

        if (opt.take && opt.skip)
            query += ' OFFSET ' + opt.skip + ' ROWS FETCH NEXT ' + opt.take + ' ROWS ONLY';
        else if (opt.take)
            query += ' FETCH FIRST ' + opt.take + ' ROWS ONLY';
        else if (opt.skip)
            query += ' OFFSET ' + opt.skip + ' ROWS';
    }

    model.query = query;
    model.params = params;

    if (CANSTATS) {
        if (isread)
            F.stats.performance.dbrm++;
        else
            F.stats.performance.dbwm++;
    }

    return model;
}

function ORACLE_ESCAPE(value) {
    if (value == null)
        return 'null';

    if (value instanceof Array) {
        let builder = [];
        for (let m of value)
            builder.push(ORACLE_ESCAPE(m));
        return builder.join(',');
    }

    let type = typeof(value);

    if (type === 'function') {
        value = value();
        if (value == null)
            return 'null';
        type = typeof(value);
    }

    if (type === 'boolean')
        return value ? '1' : '0';

    if (type === 'number')
        return value + '';

    if (type === 'string')
        return oracle_escape(value);

    if (value instanceof Date)
        return oracle_escape(dateToString(value));

    if (type === 'object')
        return oracle_escape(JSON.stringify(value));

    return oracle_escape(value.toString());
}

function oracle_escape(val) {
    if (val == null)
        return 'NULL';
    val = val.replace(REG_ORACLE_ESCAPE, "''");
    return "'" + val + "'";
}

global.ORACLE_ESCAPE = ORACLE_ESCAPE;

global.ORACLE_ESCAPE = ORACLE_ESCAPE;

function dateToString(dt) {
    var arr = [];
    arr.push(dt.getFullYear().toString());
    arr.push((dt.getMonth() + 1).toString().padStart(2, '0'));
    arr.push(dt.getDate().toString().padStart(2, '0'));
    arr.push(dt.getHours().toString().padStart(2, '0'));
    arr.push(dt.getMinutes().toString().padStart(2, '0'));
    arr.push(dt.getSeconds().toString().padStart(2, '0'));
    return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
}

exports.init = function(name, connstring, pooling, errorhandling) {
    if (!name)
        name = 'default';

    if (POOLS[name]) {
        POOLS[name].close();
        delete POOLS[name];
    }

    if (!connstring) {
        NEWDB(name, null);
        return;
    }

    var onerror = null;
    if (errorhandling)
        onerror = (err, cmd) => errorhandling(err + ' - ' + cmd.query.substring(0, 100));

    NEWDB(name, function(filter, callback) {
        filter.table2 = filter.schema ? (filter.schema + '.' + filter.table) : filter.table;
        oracledb.getConnection({ connectString: connstring }, function(err, client) {
            if (err) return callback(err);
            exec(client, filter, callback, () => client.close(), onerror);
        });
    });
};

ON('service', function(counter) {
    if (counter % 10 === 0)
        FieldsCache = {};
});
