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
    protected $filters = null;
    protected $orderBy = null;
    protected $implementedOperators=[
        "eq"=>"=",
        "lt"=>"<",
        "lte"=>"<=",
        "gt"=>">",
        "gte"=>">=",
    ];
    protected $ineqFilteredProperty = null;
    protected $orderByDirection = 'DESC';
    protected $argsDefinition = [];
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
            if($this->filters){
                if(!empty($this->filters["scalarFilters"])){
                    foreach($this->filters["scalarFilters"] as $scalarFilter){
                        if(isset($this->implementedOperators[$scalarFilter["operator"]])){
                            if($scalarFilter["operator"]!='eq'){
                                if(!$this->ineqFilteredProperty){
                                    $this->ineqFilteredProperty=$scalarFilter["field"];
                                } elseif($this->ineqFilteredProperty!=$scalarFilter["field"]){
                                    continue;
                                }
                            }
                            $query->filter($scalarFilter["field"],$this->implementedOperators[$scalarFilter["operator"]],$scalarFilter["value"]);
                        }
                    }
                }
                if(!empty($this->filters["betweenFilters"])){
                    foreach($this->filters["betweenFilters"] as $betweenFilter){
                            if($betweenFilter["operator"]=='between'){
                                if(!$this->ineqFilteredProperty){
                                    $this->ineqFilteredProperty=$betweenFilter["field"];
                                } elseif($this->ineqFilteredProperty!=$betweenFilter["field"]){
                                    continue;
                                }
                                $query->filter($betweenFilter["field"],">=",$betweenFilter["min"]);
                                $query->filter($betweenFilter["field"],"<=",$betweenFilter["max"]);
                            }
                    }
                }
            }
            $query->offset($this->start);
            $query->limit($this->limit);
            if($this->ineqFilteredProperty){
                $query->order($this->ineqFilteredProperty);
            }

            if (!empty($this->orderBy)) {
                switch ($this->orderByDirection) {
                    case "ASC":
                        $orderBy = Query::ORDER_ASCENDING;
                        break;
                    case "DESC":
                    default:
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
                if(!empty($entity)){
                    $result = $entity->get();
                    $result[$this->idField] = $entity->key()->pathEndIdentifier();
                    $this->payload['response'] = $result;
                } else {
                    $this->payload['response'] = null;
                }
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
            case 'upsert':
                try {
                    $entityKey = isset($this->args[$this->idField]) ? $this->client->key($this->config['kind'], $this->args[$this->idField]) : $this->client->key($this->config['kind']);
                    $entity = $this->client->entity(
                        $entityKey,
                        $argsList,
                        ['excludeFromIndexes'=>isset($this->config["excludeFromIndexes"]) ? $this->config["excludeFromIndexes"] : []]
                    );
                    $this->client->upsert($entity);
                    $result = $entity;
                } catch (Exception $e) {
                    $error = json_decode($e->getMessage());
                    throw new ConnectorUsage($error->error->message);
                }
                break;
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
                        $argsList,
                        ['excludeFromIndexes'=>isset($this->config["excludeFromIndexes"]) ? $this->config["excludeFromIndexes"] : []]
                    );
                    $this->client->insert($entity);
                    $result = $entity;
                } catch (Exception $e) {
                    $error = json_decode($e->getMessage());
                    throw new ConnectorUsage($error->error->message);
                }
                break;
            case 'bypass':
                $result=$this->args;
                break;
        }
        $result[$this->idField] = isset($this->args[$this->idField]) ? $this->args[$this->idField] : $result->key()->path()[0]["id"];
        $this->payload['response'] = $result;
        return $this->payload;

    }

    protected function setPayload($payload) {
         $this->payload = $payload;
         $configs = isset($payload["configs"]) ? $payload["configs"] : [];
         $baseConfig=isset($payload["connectorBaseConfig"]) ? $payload["connectorBaseConfig"] : [];
         $this->config = array_merge($baseConfig, $configs);
        $this->idField=!empty($this->config["idField"]) ? $this->idField=$this->config["idField"] : "id";
         $this->args=isset($this->payload["args"]) ? $payload["args"] : [];
         $this->multivalued=isset($payload["multivalued"]) ? $payload["multivalued"] : false;
         $this->methodName = isset($this->payload['methodName']) ? $this->payload['methodName'] : null;         if (!empty($payload['pipelineParams']['start'])) $this->start = $payload['pipelineParams']['start'];
        $this->start =!empty($payload['pipelineParams']['start']) ? $payload['pipelineParams']['start'] : 0;
        $this->limit =!empty($payload['pipelineParams']['limit']) ? $payload['pipelineParams']['limit'] : 10;
        $this->filters =!empty($payload['pipelineParams']['filters']) ? $payload['pipelineParams']['filters'] : null;
        $this->orderBy =!empty($payload['pipelineParams']['orderBy']) ? $payload['pipelineParams']['orderBy'] : null;
        $this->orderByDirection =!empty($payload['pipelineParams']['orderByDirection']) ? $payload['pipelineParams']['orderByDirection'] : 'DESC';
        $this->argsDefinition =!empty($payload['pipelineParams']['argsDefinition']) ? $payload['pipelineParams']['argsDefinition'] : [];
     }

    protected function checkConfig() {
        foreach($this->requiredConfig as $var => $msg) {
            if (empty($this->config[$var])) {
                throw new ConnectorConfig($msg);
            }
        }
    }
}
