<?php

namespace AlambicGDSConnector;

use \Google\Cloud\ServiceBuilder;
use \Google\Cloud\Datastore\Query\Query;
use \Exception;

class Connector
{
    protected $client;
    protected $payload;
    protected $config;
    protected $args;
    protected $multivalued;
    protected $start = "";
    protected $limit = 10;
    protected $orderBy = null;
    protected $orderByDirection = 'DESC';
    protected $argsDefinition;
    protected $requiredArgs = [];
    protected $connection;
    protected $requiredConfig = [
        'kind' => 'Kind name is required'
    ];

    public function __invoke($payload = [])
    {
        if (isset($payload['response'])) {
            return $payload;
        }

        $this->setPayload($payload);
        $this->checkConfig();

        $connectionParams = isset($this->config['projectId']) ? ['projectId' => $this->config['projectId']] : [];

        $this->client = Connection::getInstance($connectionParams)->getConnection();

        return $payload['isMutation'] ? $this->execute($payload) : $this->resolve($payload);
    }

    public function resolve($payload = [])
    {
        $query = $this->client->query();
        $query->kind($this->config['kind']);

        $fields = [];
        if (!empty($payload['pipelineParams']['argsDefinition'])) {
            // only query scalar types
            foreach ($payload['pipelineParams']['argsDefinition'] as $key => $value) {
                if (in_array($value['type'], ['Int', 'Float', 'Boolean', 'String', 'ID'])) {
                    $fields[] = $key;
                } else {
                    $fields[] = reset($value['relation']);
                }
            }
        }

        if (!empty($fields)) {
            //$query->projection($fields);
        }

        foreach ($this->args as $key => $value) {
            $type = isset($this->argsDefinition[$key]['type']) ? $this->argsDefinition[$key]['type'] : 'unknown';
            switch ($type) {
                case 'Int':
                case 'Float':
                case 'Boolean':
                    $query->filter($key,"=",$value);
                    break;
                case 'ID':
                    $query->filter('__key__', '=', $this->client->key($this->config['kind'], $value));
                    break;
                case 'String':
                case 'unknown':
                    $query->filter($key,"=","\"$value\"");
                    break;
            }
        }

        if ($this->multivalued) {
            $query->start($this->start);
            $query->limit($this->limit);
            if (!empty($this->orderBy)) {
                switch ($this->orderByDirection) {
                    case "ASC":
                        $orderBy = Query::ORDER_ASCENDING;
                        break;
                    case "DESC":
                        $orderBy = Query::ORDER_DESCENDING;
                        break;
                }
                $query->order($this->orderBy, $orderBy);
            }
        }

        $results = $this->client->runQuery($query);

        if ($this->multivalued) {
            if (!empty($results)) {
                foreach($results as $entity) {
                    $result = $entity->get();
                    $result['id'] = $entity->key()->pathEndIdentifier();
                    $payload['response'][] = $result;
                }
            } else {
                $payload['response'] = null;
            }
        } else {
            if (!empty($results)) {
                $entity = $results->current();
                $result = $entity->get();
                $result['id'] = $entity->key()->pathEndIdentifier();
                $payload['response'] = $result;
            } else {
                $payload['response'] = null;
            }
        }

        return $payload;
    }
    public function execute($payload = [])
    {
        $args = isset($payload['args']) ? $payload['args'] : [];
        $basePath = $payload['configs']['path'];
        $methodName = isset($payload['methodName']) ? $payload['methodName'] : null;
        if (empty($methodName)) {
            throw new Exception('Firebase connector requires a valid methodName for write ops');
        }
        $argsList = $args;
        unset($argsList['id']);
        switch ($methodName) {
            case 'update':
                try {
                    $path = $basePath.'/'.$args['id'];
                    $data = $this->_client->update($path, $argsList);
                    $result = json_decode($data, true);
                } catch (Exception $exception) {
                    throw new Exception($exception->getMessage());
                }
                break;
            case 'delete':
                try {
                    $path = $basePath.'/'.$args['id'];
                    $data = $this->_client->delete($path);
                    $result = json_decode($data, true);
                } catch (Exception $exception) {
                    throw new Exception($exception->getMessage());
                }
                break;
            case 'create':
                try {
                    if (isset($args['id'])) {
                        $path = $basePath.'/'.$args['id'];
                        $data = $this->_client->set($path, $argsList);
                        $result = json_decode($data, true);
                    } else {
                        $path = $basePath;
                        $data = $this->_client->push($path, $argsList);
                        $id = json_decode($data)->name;
                        $args['id'] = $id;
                        $result = $argsList;
                    }
                } catch (Exception $exception) {
                    throw new Exception($exception->getMessage());
                }
                break;
            default:
                throw new Exception("Error: unknown $methodName mutation");
        }
        if (!isset($result['error'])) {
            $result['id'] = $args['id'];
            $payload['response'] = $result;
            return $payload;
        } else {
            throw new Exception('Firebase error: '.$result['error']);
        }
    }

    protected function setPayload($payload) {
         $this->payload = $payload;
         $configs = isset($payload["configs"]) ? $payload["configs"] : [];
         $baseConfig=isset($payload["connectorBaseConfig"]) ? $payload["connectorBaseConfig"] : [];
         $this->config = array_merge($baseConfig, $configs);
         $this->args=isset($this->payload["args"]) ? $payload["args"] : [];
         $this->multivalued=isset($payload["multivalued"]) ? $payload["multivalued"] : false;
         if (!empty($payload['pipelineParams']['start'])) $this->start = $payload['pipelineParams']['start'];
         if (!empty($payload['pipelineParams']['limit'])) $this->limit = $payload['pipelineParams']['limit'];
         if (!empty($payload['pipelineParams']['orderBy'])) $this->orderBy = $payload['pipelineParams']['orderBy'];
         if (!empty($payload['pipelineParams']['orderByDirection'])) $this->orderByDirection = $payload['pipelineParams']['orderByDirection'];
         if (!empty($payload['pipelineParams']['argsDefinition'])) $this->argsDefinition = $payload['pipelineParams']['argsDefinition'];
     }

    protected function checkConfig() {
        foreach($this->requiredConfig as $var => $msg) {
            if (empty($this->config[$var])) {
                throw new ConnectorConfig($msg);
            }
        }
    }
}
