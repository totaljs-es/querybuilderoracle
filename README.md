# Total.js QueryBuilder: Oracle <img src="https://github.com/user-attachments/assets/45918196-9ddd-4c4a-bacf-e5ade11ad1b0" alt="Oracle Logo" heigth="14" style="vertical-align: middle;" />
A simple QueryBuilder integrator for Oracle database.

- [Documentation](https://docs.totaljs.com/total4/pzbr001pr41d/)
- `$ npm install @totaljs-es/querybuilderoracle@0.8.0`

## Initialization

- Example: `oracle://user:password@localhost:port/service`

```js
// require('querybuilderoracle').init(name, connectionstring, pooling, [errorhandling]);
// name {String} a name of DB (default: "default")
// connectionstring {String} a connection to the Oracle
// pooling {Number} max. clients (default: "0" (disabled))
// errorhandling {Function(err, cmd)}

require('querybuilderoracle').init('default', CONF.database);
// require('querybuilderoracle').init('default', CONF.database, 10);
```

__Usage__:

```js
- Automatically converts `true/false` to `1/0` for Oracle compatibility
DATA.find('tbl_user').where('id', 1234).where('isactive', true).callback(console.log);
```

## Connection string attributes

- Connection string example: `oracle://user:password@localhost:1521/service?schema=parking&pooling=2`

---

- `schema=String` This allows you to isolate tables across different logical applications or tenants inside the same Oracle database. If omitted, queries will run using the Oracle user’s default schema.
- `pooling=Number` sets a default pooling (it overwrites pooling)
