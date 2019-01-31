import test from 'ava';
import mysql from 'mysql';

test.cb('can connect to go-mysql-server', t => {
    const connection = mysql.createConnection({
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: '',
        database: 'mydb'
    });

    connection.connect();

    const query = 'SELECT name, email FROM mytable ORDER BY name, email';
    const expected = [
        { name: "Evil Bob", email: "evilbob@gmail.com" },
        { name: "Jane Doe", email: "jane@doe.com" },
        { name: "John Doe", email: "john@doe.com" },
        { name: "John Doe", email: "johnalt@doe.com" },
    ];

    connection.query(query, function (error, results, _) {
        if (error) throw error;

        const rows = results.map(r => ({ name: r.name, email: r.email }));
        t.deepEqual(rows, expected);
        t.end();
    });

    connection.end();
});
