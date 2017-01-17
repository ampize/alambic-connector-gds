<?php

namespace AlambicGDSConnector;

use \Google\Cloud\ServiceBuilder;
use \Google\Cloud\Datastore\Query\Query;

use Alambic\Exception\ConnectorArgs;
use Alambic\Exception\ConnectorConfig;
use Alambic\Exception\ConnectorUsage;
use \Exception;

class Connector
{
    protected $client;
    protected $payload;
    protected $config;
    protected $idField="id";
    protected $args;
    protected $multivalued;
    protected $start = "";
    protected $limit = 10;
    protected $orderBy = null;
    protected $orderByDirection = 'DESC';
    protected $argsDefinition;
    protected $requiredArgs = [];
    protected $connection;
    protected $methodName;
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
        $connectionParams=[];
        if(isset($this->config['projectId'])){
            $connectionParams["projectId"]=$this->config['projectId'];
        }
        if(isset($this->config['namespaceId'])){
            $connectionParams["namespaceId"]=$this->config['namespaceId'];
        }

        $this->client = Connection::getInstance($connectionParams)->getConnection();

        return $payload['isMutation'] ? $this->execute($payload) : $this->resolve();
    }

    public function resolve()
    {
        $query = $this->client->query();
        $query->kind($this->config['kind']);

        foreach ($this->args as $key => $value) {
            $type = isset($this->argsDefinition[$key]['type']) ? $this->argsDefinition[$key]['type'] : 'unknown';
            switch ($type) {
                case 'Int':
                case 'Float':
                case 'Boolean':
                case 'String':
                case 'unknown':
                    $query->filter($key,"=",$value);
                    break;
                case 'ID':
                    $query->filter('__key__', '=', $this->client->key($this->config['kind'], $value));
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
                $this->payload['response']=[];
                foreach($results as $entity) {
                    $result = $entity->get();
                    $result[$this->idField] = $entity->key()->pathEndIdentifier();
                    $this->payload['response'][] = $result;
                }
            } else {
                $this->payload['response'] = null;
            }
        } else {
            if (!empty($results)) {
                $entity = $results->current();
                $result = $entity->get();
                $result[$this->idField] = $entity->key()->pathEndIdentifier();
                $this->payload['response'] = $result;
            } else {
                $this->payload['response'] = null;
            }
        }

        return $this->payload;
    }
    public function execute()
    {

        if(empty($this->methodName)){
            throw new ConnectorConfig('MongoDB connector requires a valid methodName for write ops');
        }
        if(empty($this->args[$this->idField])&&$this->methodName!='create'){
            throw new ConnectorArgs('MongoDB connector requires id for operations other than create');
        }
        $argsList = $this->args;
        unset($argsList['id']);
        switch ($this->methodName) {
            case 'update':
                try {
                    $entityKey = $this->client->key($this->config['kind'], $this->args[$this->idField]);
                    $transaction = $this->client->transaction();
                    $entity = $transaction->lookup($entityKey);
                    if (!is_null($entity)) {
                        foreach($argsList as $key => $value) {
                            $entity[$key] = $value;
                        }
                        $transaction->upsert($entity);
                        $transaction->commit();
                        $result = $entity;
                    } else {
                        throw new ConnectorUsage("Record not found: ".$this->args[$this->idField]);
                    }
                } catch (Exception $e) {
                    throw new ConnectorUsage($e->getMessage());
                }
                break;
            case 'delete':
                try {
                    $entityKey = $this->client->key($this->config['kind'], $this->args[$this->idField]);
                    $this->client->delete($entityKey);
                } catch (Exception $e) {
                    $error = json_decode($e->getMessage());
                    throw new ConnectorUsage($error->error->message);
                }
                break;
            case 'create':
                try {
                    $entityKey = isset($this->args[$this->idField]) ? $this->client->key($this->config['kind'], $this->args[$this->idField]) : $this->client->key($this->config['kind']);
                    $entity = $this->client->entity(
                        $entityKey,
                        $argsList
                    );
                    $this->client->insert($entity);
                    $result = $entity;
                } catch (Exception $e) {
                    $error = json_decode($e->getMessage());
                    throw new ConnectorUsage($error->error->message);
                }
                break;
        }
        $result[$this->idField] = $this->methodName=="create" ? $result->key()->path()[0]["id"] : $this->args[$this->idField];
        $payload['response'] = $result;
        return $payload;

    }

    protected function setPayload($payload) {
         $this->payload = $payload;
         $configs = isset($payload["configs"]) ? $payload["configs"] : [];
         $baseConfig=isset($payload["connectorBaseConfig"]) ? $payload["connectorBaseConfig"] : [];
         $this->config = array_merge($baseConfig, $configs);
         if(!empty($this->config["idField"])){
             $this->idField=$this->config["idField"];
         }
         $this->args=isset($this->payload["args"]) ? $payload["args"] : [];
         $this->multivalued=isset($payload["multivalued"]) ? $payload["multivalued"] : false;
         $this->methodName = isset($this->payload['methodName']) ? $this->payload['methodName'] : null;         if (!empty($payload['pipelineParams']['start'])) $this->start = $payload['pipelineParams']['start'];
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
