<?php

namespace AlambicGDSConnector;

use \Google\Cloud\ServiceBuilder;

class Connection
{

    private static $instances = [];
    private $connectionParams;
    private $connection;

    private function __construct($connectionParams)
    {
        $cloud = new ServiceBuilder();
        $this->connection = $cloud->datastore($connectionParams);
    }

    public static function getInstance($connectionParams)
    {
        $key = serialize($connectionParams);
        if (!array_key_exists($key, self::$instances)) {
            self::$instances[$key] = new self($connectionParams);
        }
        return self::$instances[$key];
    }

    public function getConnection()
    {
        return $this->connection;
    }
}
