// Total.js Module: Oracle integrator
var oracledb = require('oracledb');
var CANSTATS = global.F ? (global.F.stats && global.F.stats.performance && global.F.stats.performance.dbrm != null) : false;
var REG_LANGUAGE = /[a-z0-9]+§/gi;
var REG_COL_TEST = /"|\s|:|\./;
var REG_WRITE = /(INSERT|UPDATE|DELETE|DROP)/i;
var LOGGER = '-- ORACLE -->';
var POOLS = {};
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

		client.execute(cmd.query, cmd.params || [], { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: true }, function(err, response) {
			if (err) {
				done();
				errorhandling && errorhandling(err, cmd);
				callback(err);
			} else {
				cmd = makesql(filter, 'count');

				if (filter.debug)
					console.log(LOGGER, cmd.query, cmd.params);

				client.execute(cmd.query, cmd.params || [], { outFormat: oracledb.OUT_FORMAT_OBJECT }, function(err, counter) {
					done();
					err && errorhandling && errorhandling(err, cmd);
					callback(err, err ? null : { items: response.rows, count: +counter.rows[0].COUNT });
				});
			}
		});
		return;
	}

	try {
		cmd = makesql(filter);
		console.log(cmd);
	} catch (e) {
		done();
		callback(e);
		return;
	}

	if (filter.debug)
		console.log(LOGGER, cmd.query, cmd.params);

	client.execute(cmd.query, cmd.params || [], { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: true }, function(err, response) {
		done();

		if (err) {
			errorhandling && errorhandling(err, cmd);
			callback(err);
			return;
		}

		var output;
		var rows = response.rows || [];

		switch (filter.exec) {
			case 'insert':
				if (filter.returning)
					output = rows.length && rows[0];
				else if (filter.primarykey)
					output = rows.length && rows[0][filter.primarykey.toUpperCase()];
				else
					output = response.rowsAffected;
				callback(null, output);
				break;
			case 'update':
				if (filter.returning)
					output = filter.first ? (rows.length && rows[0]) : rows;
				else
					output = rows.length && rows[0] && rows[0].COUNT || 0;
				callback(null, output);
				break;
			case 'remove':
				if (filter.returning)
					output = filter.first ? (rows.length && rows[0]) : rows;
				else
					output = response.rowsAffected;
				callback(null, output);
				break;
			case 'check':
				output = rows.length > 0 && rows[0].COUNT > 0;
				callback(null, output);
				break;
			case 'count':
				output = rows.length ? rows[0].COUNT : null;
				callback(null, output);
				break;
			case 'scalar':
				output = filter.scalar.type === 'group' ? rows : (rows[0] ? rows[0].VALUE : null);
				callback(null, output);
				break;
			default:
				callback(null, rows);
				break;
		}
	});
}

var valueparse = function(value) {
	let type = typeof value;
	
	if (type === 'string')
		value = value.replace(/=FALSE/gi, '=0').replace(/=TRUE/gi, '=1');

	if (type === 'boolean')
		value = value ? 1 : 0;
	
	return value;
};

function oracle_where(where, opt, filter, operator) {
	var tmp;
	for (var item of filter) {
		var name = '';
		if (item.name) {
			var key = 'where_' + (opt.language || '') + '_' + item.name;
			name = FieldsCache[key];
			if (!name) {
				name = item.name;
				if (name[name.length - 1] === '§')
					name = replacelanguage(item.name, opt.language, true);
				else
					name = REG_COL_TEST.test(name) ? name : '"' + name + '"';
				FieldsCache[key] = name;
			}
		}

		if (typeof item.value === 'boolean')
			item.value = item.value ? 1 : 0;
		
		switch (item.type) {
			case 'or':
				tmp = [];
				oracle_where(tmp, opt, item.value, 'OR');
				where.length && where.push(operator);
				where.push('(' + tmp.join(' ') + ')');
				break;
			case 'in':
			case 'notin':
				where.length && where.push(operator);
				tmp = [];
				if (item.value instanceof Array) {
					for (var val of item.value) {
						if (val != null)
							tmp.push(oracle_escape(val));
					}
				} else if (item.value != null)
					tmp = [oracle_escape(item.value)];
				if (!tmp.length)
					tmp.push('NULL');
				where.push(name + (item.type === 'in' ? ' IN ' : ' NOT IN ') + '(' + tmp.join(',') + ')');
				break;
			case 'query':
				where.length && where.push(operator);
				where.push('(' + item.value + ')');
				break;
			case 'where':
				where.length && where.push(operator);
				if (item.value == null)
					where.push(name + (item.comparer === '=' ? ' IS NULL' : ' IS NOT NULL'));
				else
					where.push(name + item.comparer + oracle_escape(item.value));
				break;
			case 'contains':
				where.length && where.push(operator);
				where.push('LENGTH(' + name + ')>0');
				break;
			case 'search':
				where.length && where.push(operator);
				tmp = item.value ? item.value.replace(/%/g, '') : '';
				if (item.operator === 'beg')
					where.push('UPPER(' + name + ') LIKE ' + oracle_escape(tmp.toUpperCase() + '%'));
				else if (item.operator === 'end')
					where.push('UPPER(' + name + ') LIKE ' + oracle_escape('%' + tmp.toUpperCase()));
				else
					where.push('UPPER(' + name + ') LIKE ' + oracle_escape('%' + tmp.toUpperCase() + '%'));
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				where.length && where.push(operator);
				where.push('EXTRACT(' + item.type.toUpperCase() + ' FROM ' + name + ')' + item.comparer + oracle_escape(item.value));
				break;
			case 'empty':
				where.length && where.push(operator);
				where.push('(' + name + ' IS NULL OR LENGTH(' + name + ')=0)');
				break;
			case 'between':
				where.length && where.push(operator);
				where.push('(' + name + ' BETWEEN ' + oracle_escape(item.a) + ' AND ' + oracle_escape(item.b) + ')');
				break;
			case 'permit':
				where.length && where.push(operator);
				tmp = [];
				for (var m of item.value)
					tmp.push(oracle_escape(m));
				if (!tmp.length)
					tmp = ['NULL'];
				if (item.required)
					where.push('(' + (item.userid ? ('userid=' + oracle_escape(item.userid) + ' OR ') : '') + name + ' IS NULL OR ' + name + ' IN (' + tmp.join(',') + '))');
				else
					where.push('(' + (item.userid ? ('userid=' + oracle_escape(item.userid) + ' OR ') : '') + name + ' IN (' + tmp.join(',') + '))');
				break;
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
				} else
					query.push('"' + key + '" = NVL("' + key + '",0) ' + c + ' :' + params.length);
				break;
			case '>':
			case '<':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('"' + key + '"');
					query.push(':' + params.length);
				} else
					query.push('"' + key + '" = ' + (c === '>' ? 'GREATEST' : 'LEAST') + '("' + key + '", :' + params.length + ')');
				break;
			case '!':
				key = key.substring(1);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('0');
				} else
					query.push('"' + key + '" = CASE "' + key + '" WHEN 1 THEN 0 ELSE 1 END');
				break;
			case '=':
			case '#':
				key = key.substring(1);
				if (insert) {
					if (c === '=') {
						fields.push('"' + key + '"');
						query.push(val);
					}
				} else
					query.push('"' + key + '" = ' + val);
				break;
			default:
				params.push(val);
				if (insert) {
					fields.push('"' + key + '"');
					query.push(':' + params.length);
				} else
					query.push('"' + key + '" = :' + params.length);
				break;
		}
	}

	return { fields: fields, query: query, params: params };
}

function replacelanguage(fields, language, noas) {
	return fields.replace(REG_LANGUAGE, function(val) {
		val = val.substring(0, val.length - 1);
		return '"' + val + '' + (noas ? ((language || '') + '"') : language ? (language + '" AS \"' + val + '\"') : '"');
	});
}

function quotefields(fields) {
	if (!fields || fields === '*')
		return fields;
console.log(fields);
	return fields.split(',').map(x => {
		x = x.trim();
		if (!x.length)
			return '';
		if (x[0] === '"')
			return x;
		return '"' + x + '"';
	}).join(',');
}

function makesql(opt, exec) {
	var query = '';
	var where = [];
	var model = {};
	var isread = false;
	var params;
	var returning;
	var tmp;

	if (opt.schema)
		opt.table2 = '"' + opt.schema.replace(/"/g, '') + '"."' + opt.table.replace(/"/g, '') + '"';
	else
		opt.table2 = '"' + opt.table.replace(/"/g, '') + '"';

	if (!exec)
		exec = opt.exec;

	oracle_where(where, opt, opt.filter, 'AND');

	var language = opt.language || '';
	var fields;
	var sort;

	if (opt.fields) {
		var key = 'fields_' + language + '_' + opt.fields.join(',');
		fields = FieldsCache[key] || '';
		if (!fields) {
			for (var i = 0; i < opt.fields.length; i++) {
				var m = opt.fields[i];
				if (m[m.length - 1] === '§')
					fields += (fields ? ',' : '') + replacelanguage(m, language);
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
			returning = opt.returning ? opt.returning.join(',') : opt.primarykey || '';
			tmp = oracle_insertupdate(opt, true);
			query = 'INSERT INTO ' + opt.table2 + ' (' + tmp.fields.join(',') + ') VALUES (' + tmp.query.join(',') + ')';
			if (returning)
				query += ' RETURNING ' + returning + ' INTO ' + returning.split(',').map(f => ':' + (tmp.params.length + 1)).join(',');
			params = tmp.params;
			break;
		case 'remove':
			returning = opt.returning ? opt.returning.join(',') : opt.primarykey || '';
			query = 'DELETE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			if (returning)
				query += ' RETURNING ' + returning + ' INTO ' + returning.split(',').map(f => ':' + (params ? params.length + 1 : 1)).join(',');
			break;
		case 'update':
			returning = opt.returning ? opt.returning.join(',') : '';
			tmp = oracle_insertupdate(opt);
			query = 'UPDATE ' + opt.table2 + ' SET ' + tmp.query.join(',') + (where.length ? (' WHERE ' + where.join(' ')) : '');
			if (returning)
				query += ' RETURNING ' + returning + ' INTO ' + returning.split(',').map(f => ':' + (tmp.params.length + 1)).join(',');
			params = tmp.params;
			break;
		case 'check':
			query = 'SELECT COUNT(1) AS COUNT FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'drop':
			query = 'DROP TABLE ' + opt.table2;
			break;
		case 'truncate':
			query = 'TRUNCATE TABLE ' + opt.table2;
			break;
		case 'command':
			break;
		case 'scalar':
			opt.first = true;
			var val = opt.scalar.key === '*' ? '1' : opt.scalar.key;
			if (opt.scalar.type === 'group') {
				query = 'SELECT ' + val + ', ' + (opt.scalar.key2 ? ('SUM(' + opt.scalar.key2 + ')') : 'COUNT(1)') + ' AS VALUE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '') + ' GROUP BY ' + val;
			} else {
				query = 'SELECT ' + opt.scalar.type.toUpperCase() + '(' + val + ') AS VALUE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			}
			isread = true;
			break;
		case 'query':
			if (where.length) {
				var wherem = opt.query.match(/\{where\}/ig);
				var wherec = 'WHERE ' + where.join(' ');
				query = wherem ? opt.query.replace(wherem, wherec) : (opt.query + ' ' + wherec);
			} else
				query = opt.query;
			params = opt.params;
			isread = REG_WRITE.test(query) ? false : true;
			break;
	}

	if (exec === 'find' || exec === 'read' || exec === 'list' || exec === 'query' || exec === 'check') {
		if (opt.sort) {
			var key = 'sort_' + language + '_' + opt.sort.join(',');
			sort = FieldsCache[key] || '';
			if (!sort) {
				for (var i = 0; i < opt.sort.length; i++) {
					var m = opt.sort[i];
					var index = m.lastIndexOf('_');
					var name = m.substring(0, index);
					var value = (REG_COL_TEST.test(name) ? name : ('"' + name + '"')).replace(/§/, language);
					sort += (sort ? ',' : '') + value + ' ' + (m.substring(index + 1).toLowerCase() === 'desc' ? 'DESC' : 'ASC');
				}
				FieldsCache[key] = sort;
			}
			query += ' ORDER BY ' + sort;
		}

		if (opt.take || opt.skip) {
			var offset = opt.skip || 0;
			var limit = opt.take || 1000;
			query += ' OFFSET ' + offset + ' ROWS FETCH NEXT ' + limit + ' ROWS ONLY';
		}
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
		return 'NULL';

	if (value instanceof Array) {
		var builder = [];
		for (var m of value)
			builder.push(ORACLE_ESCAPE(m));
		return builder.join(',');
	}

	var type = typeof value;

	if (type === 'function') {
		value = value();
		if (value == null)
			return 'NULL';
		type = typeof value;
	}

	if (type === 'boolean')
		return value ? '1' : '0';

	if (type === 'number')
		return value.toString();

	if (type === 'string')
		return oracle_escape(value);

	if (value instanceof Date)
		return "TO_DATE('" + dateToString(value) + "', 'YYYY-MM-DD HH24:MI:SS')";

	if (type === 'object')
		return oracle_escape(JSON.stringify(value));

	return oracle_escape(value.toString());
}

// Oracle-safe escape function
function oracle_escape(val) {
	if (val == null)
		return 'NULL';
	
	return "'" + val.replace(/'/g, "''") + "'";
}

function dateToString(dt) {
	var arr = [];
	arr.push(dt.getFullYear().toString());
	arr.push((dt.getMonth() + 1).toString());
	arr.push(dt.getDate().toString());
	arr.push(dt.getHours().toString());
	arr.push(dt.getMinutes().toString());
	arr.push(dt.getSeconds().toString());

	for (var i = 1; i < arr.length; i++) {
		if (arr[i].length === 1)
			arr[i] = '0' + arr[i];
	}

	return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
}

global.ORACLE_ESCAPE = ORACLE_ESCAPE;

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
		onerror = function(err, cmd) {
			errorhandling(err + ' - ' + (cmd.query || '').substring(0, 100));
		};

	var index = connstring.indexOf('?');
	var defschema = '';
	if (index !== -1) {
		var args = connstring.substring(index + 1).parseEncoded();
		defschema = args.schema;
		if (args.pooling)
			pooling = +args.pooling;
	}

	NEWDB(name, function(filter, callback) {
		if (filter.schema == null && defschema)
			filter.schema = defschema;
		filter.table2 = filter.schema ? (filter.schema + '.' + filter.table) : filter.table;

		var conn = require('url').parse(connstring);
		var auth = conn.auth ? conn.auth.split(':') : [];

		var config = {
		  user: auth[0],
		  password: auth[1],
		  connectString: conn.host + (conn.pathname || '')
		};

		oracledb.getConnection(config, function(err, connection) {
			if (err)
				callback(err);
			else
				exec(connection, filter, callback, function() {
					connection.close();
				}, onerror);
		});
	});
};

ON('service', function(counter) {
    if (counter % 10 === 0)
        FieldsCache = {};
});
