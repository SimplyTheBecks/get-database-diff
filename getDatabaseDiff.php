<?php

/**
 * Class DatabaseDiff - класс для вычисления расхождения баз данных
 *
 * Проверяется расхождение в схемах, таблицах, столбцах и типах данных столбцов
 */
class DatabaseDiff
{
    // подключение к БД сервера Master
    private $masterServerDBConnection = false;
    // подключение к БД сервера Slave
    private $slaveServerDBConnection = false;

    /**
     * Конструктор
     *
     * @param array $masterServerOptions - параметры сервера Master
     * @param array $slaveServerOptions - параметры сервера Slave
     */
    public function __construct(array $masterServerOptions, array $slaveServerOptions)
    {
        // подключение к БД
        $this->connectToDatabase('master', $masterServerOptions);
        // подключение к БД
        $this->connectToDatabase('slave', $slaveServerOptions);
    }

    /**
     * Подключение к БД
     *
     * @param string $serverType - тип сервера
     * @param array $serverOptions - параметры сервера
     * @return boolean
     */
    private function connectToDatabase(string $serverType, array $serverOptions): bool
    {
        $res = false;

        if (empty($serverType) || empty($serverOptions)) return $res;

        if (
            !empty($serverOptions['ip'])
            && !empty($serverOptions['dbPort'])
            && !empty($serverOptions['dbName'])
            && !empty($serverOptions['dbUser'])
            && array_key_exists('dbUserPassword', $serverOptions)
        ) {
            // строка подключения к БД сервера
            $dbConnectionString =
                'host=' . $serverOptions['ip'] . ' ' .
                'port=' . $serverOptions['dbPort'] . ' ' .
                'dbname=' . $serverOptions['dbName'] . ' ' .
                'user=' . $serverOptions['dbUser'] . ' ' .
                'password=' . $serverOptions['dbUserPassword'] . ' ';
        }

        switch ($serverType) {

            case 'master':

                // подключение к БД сервера Master
                $this->masterServerDBConnection = pg_connect($dbConnectionString);

                if ($this->masterServerDBConnection !== false) $res = true;

                break;

            case 'slave':

                // подключение к БД сервера Slave
                $this->slaveServerDBConnection = pg_connect($dbConnectionString);

                if ($this->slaveServerDBConnection !== false) $res = true;

                break;
        }

        return $res;
    }

    /**
     * Вычисление расхождения баз данных
     *
     * @return array
     */
    public function execute(): array
    {
        $res = [
            'result'    => 0,
            'data'      => [],
            'messages'  => [
                'info'  => [],
                'error' => []
            ]
        ];

        // проверка соединения с БД сервера Master
        if (!$this->masterServerDBConnection) return ['result' => 0, 'data' => [], 'messages' => ['info' => [], 'error' => ['Ошибка при подключении к БД сервера Master']]];
        // проверка соединения с БД сервера Slave
        if (!$this->slaveServerDBConnection) return ['result' => 0, 'data' => [], 'messages' => ['info' => [], 'error' => ['Ошибка при подключении к БД сервера Slave']]];

        $res['result'] = 1;

        // получение структуры БД сервера Master
        $masterServerDBStructure = $this->getDatabaseStructure($this->masterServerDBConnection);
        // получение структуры БД сервера Slave
        $slaveServerDBStructure = $this->getDatabaseStructure($this->slaveServerDBConnection);

        // получение расхождения баз данных
        $res['data']['notExistOnMasterServerDB'] = $this->getDatabaseDiff($slaveServerDBStructure, $masterServerDBStructure);
        // получение расхождения баз данных
        $res['data']['notExistOnSlaveServerDB'] = $this->getDatabaseDiff($masterServerDBStructure, $slaveServerDBStructure);

        return $res;
    }

    /**
     * Получение структуры базы данных
     *
     * @param resource $dbConnection - подключение к БД
     * @return array
     */
    private function getDatabaseStructure($dbConnection): array
    {
        $res = [];

        if (empty($dbConnection) || !is_resource($dbConnection)) return $res;

        $request = "SELECT
                            t_columns.table_schema,
                            t_columns.table_name,
                            t_columns.column_name,
                            CASE
                                WHEN t_columns.character_maximum_length IS NOT NULL 
                                    THEN t_columns.data_type || '(' || t_columns.character_maximum_length || ')'
                                ELSE t_columns.data_type
                            END AS data_type
                        FROM pg_tables AS t_tables
                        LEFT JOIN information_schema.columns AS t_columns
                            ON t_tables.schemaname = t_columns.table_schema
                                AND t_tables.tablename = t_columns.table_name
                        WHERE t_columns.table_schema NOT IN (
                                'information_schema',
                                'pg_catalog'
                            )
                        ORDER BY t_columns.table_schema, t_columns.table_name, t_columns.column_name;";

        $result = pg_query($dbConnection, $request);

        if (!$result) return $res;

        while ($row = pg_fetch_assoc($result)) {

            if (!array_key_exists($row['table_schema'], $res))
                $res[$row['table_schema']] = [];

            if (!array_key_exists($row['table_name'], $res[$row['table_schema']]))
                $res[$row['table_schema']][$row['table_name']] = [];

            $res[$row['table_schema']][$row['table_name']][$row['column_name']] = $row['data_type'];
        }

        return $res;
    }

    /**
     * Получение расхождения баз данных
     *
     * @param array $firstDBStructure - структура первой БД
     * @param array $secondDBStructure - структура второй БД
     * @return array
     */
    private function getDatabaseDiff(array $firstDBStructure, array $secondDBStructure): array
    {
        $res = [
            'schemas'                   => [],
            'tables'                    => [],
            'columns'                   => [],
            'columnsWithDiffDataType'   => []
        ];

        if (empty($firstDBStructure) || empty($secondDBStructure)) return $res;

        // получение расхождения схем БД
        $res['schemas'] = $this->getDBSchemasDiff($firstDBStructure, $secondDBStructure);

        foreach ($firstDBStructure as $firstDBSchema => $firstDBTables) {

            if (in_array($firstDBSchema, $res['schemas'])) continue;

            // получение расхождения таблиц БД
            $dbTablesDiff = $this->getDBTablesDiff($firstDBSchema, $firstDBTables, $secondDBStructure[$firstDBSchema]);

            $res['tables'] = array_merge($res['tables'], $dbTablesDiff);

            foreach ($firstDBTables as $firstDBTable => $firstDBColumns) {

                if (in_array($firstDBSchema . '.' . $firstDBTable, $res['tables'])) continue;

                // получение расхождения столбцов БД
                $dbColumnsDiff = $this->getDBColumnsDiff($firstDBSchema, $firstDBTable, $firstDBColumns, $secondDBStructure[$firstDBSchema][$firstDBTable]);

                $res['columns'] = array_merge($res['columns'], $dbColumnsDiff);

                foreach ($firstDBColumns as $firstDBColumn => $firstDBColumnDataType) {

                    if (in_array($firstDBSchema . '.' . $firstDBTable . '.' . $firstDBColumn, $res['columns'])) continue;

                    // // получение расхождения типов данных столбцов БД
                    if ($firstDBColumnDataType !== $secondDBStructure[$firstDBSchema][$firstDBTable][$firstDBColumn]) {

                        $res['columnsWithDiffDataType'][] = $firstDBSchema . '.' . $firstDBTable . '.' . $firstDBColumn .
                            ' (' . $firstDBColumnDataType . ' != ' . $secondDBStructure[$firstDBSchema][$firstDBTable][$firstDBColumn] . ')';
                    }
                }
            }
        }

        return $res;
    }

    /**
     * Получение расхождения схем БД
     *
     * @param array $firstDBStructure - структура первой БД
     * @param array $secondDBStructure - структура второй БД
     * @return array
     */
    private function getDBSchemasDiff(array $firstDBStructure, array $secondDBStructure): array
    {
        $res = [];

        if (empty($firstDBStructure) || empty($secondDBStructure)) return $res;

        $firstDBSchemasArr = array_keys($firstDBStructure);
        $secondDBSchemasArr = array_keys($secondDBStructure);

        $res = array_values(array_diff($firstDBSchemasArr, $secondDBSchemasArr));

        return $res;
    }

    /**
     * Получение расхождения таблиц БД
     *
     * @param string $firstDBSchema - схема первой БД
     * @param array $firstDBTables - таблицы первой БД
     * @param array $secondDBTables - таблицы второй БД
     * @return array
     */
    private function getDBTablesDiff(string $firstDBSchema, array $firstDBTables, array $secondDBTables): array
    {
        $res = [];

        if (empty($firstDBSchema) || empty($firstDBTables) || empty($secondDBTables)) return $res;

        $firstDBTablesArr = array_keys($firstDBTables);
        $secondDBTablesArr = array_keys($secondDBTables);

        $res = array_values(array_diff($firstDBTablesArr, $secondDBTablesArr));

        foreach ($res as &$tableName) {

            $tableName = $firstDBSchema . '.' . $tableName;
        }

        return $res;
    }

    /**
     * Получение расхождения столбцов БД
     *
     * @param string $firstDBSchema - схема первой БД
     * @param string $firstDBTable - таблица первой БД
     * @param array $firstDBColumns - столбцы первой БД
     * @param array $secondDBColumns - столбцы второй БД
     * @return array
     */
    private function getDBColumnsDiff(string $firstDBSchema, string $firstDBTable, array $firstDBColumns, array $secondDBColumns): array
    {
        $res = [];

        if (empty($firstDBSchema) || empty($firstDBTable) || empty($firstDBColumns) || empty($secondDBColumns)) return $res;

        $firstDBColumnsArr = array_keys($firstDBColumns);
        $secondDBColumnsArr = array_keys($secondDBColumns);

        $res = array_values(array_diff($firstDBColumnsArr, $secondDBColumnsArr));

        foreach ($res as &$columnName) {

            $columnName = $firstDBSchema . '.' . $firstDBTable . '.' . $columnName;
        }

        return $res;
    }
}
