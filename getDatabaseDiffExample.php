<?php

require_once(__DIR__ . '/class/DatabaseDiff.php');

// параметры для подключения к БД сервера Master
$masterServerOptions = [
    'ip'             => '127.0.0.1',
    'dbPort'         => '5432',
    'dbName'         => 'postgres',
    'dbUser'         => 'postgres',
    'dbUserPassword' => ''
];

// параметры для подключения к БД сервера Slave
$slaveServerOptions = [
    'ip'             => '127.0.0.1',
    'dbPort'         => '5432',
    'dbName'         => 'postgres',
    'dbUser'         => 'postgres',
    'dbUserPassword' => ''
];

// вычисление расхождения баз данных (PostgreSQL)
$databaseDiff = new DatabaseDiff($masterServerOptions, $slaveServerOptions);
$res = $databaseDiff->execute();

echo json_encode($res, JSON_UNESCAPED_UNICODE);