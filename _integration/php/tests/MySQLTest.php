<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;

final class MySQLTest extends TestCase
{
    public function testConnection(): void {
        try {
            $conn = new PDO("mysql:host=127.0.0.1:3306;dbname=mydb", "root", "");
            $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

            $stmt = $conn->query('SELECT name, email FROM mytable ORDER BY name, email');
            $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $expected = [
                ["name" => "Evil Bob", "email" => "evilbob@gmail.com"],
                ["name" => "Jane Doe", "email" => "jane@doe.com"],
                ["name" => "John Doe", "email" => "john@doe.com"],
                ["name" => "John Doe", "email" => "johnalt@doe.com"],
            ];

            $this->assertEquals($expected, $result);
        } catch (\PDOException $e) {
            $this->assertFalse(true, $e->getMessage());
        }
    }
}
