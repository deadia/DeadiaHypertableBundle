<?php

namespace Deadia\HypertableBundle\Driver;
use Symfony\Component\Yaml\Parser;

require_once(__DIR__.'/ThriftClient.php');

/**
* Hypertable Base class
*/
class Driver extends \Hypertable_ThriftClient{
	/**
	* Instance
	*/
	static private $instance = NULL;
	
	/**
	* Hypertable namespace
	*/
	protected $ns;
	
	/**
	* Hypertable table
	*/
	protected $table = NULL;
	
	/**
	* Host
	*/
	private $host;
	
	/**
	* port
	*/
	private $port;
	
	/**
	* namespace name
	*/
	private $ns_name;
	
	/**
	* Constructor
	*/
	public function __construct()
	{
		$yaml = new Parser();

		$value = $yaml->parse(file_get_contents(__DIR__.'/../../../../../../app/config/parameters.yml'));
		$this->host = isset($value['parameters']['hypertable_host']) ? $value['parameters']['hypertable_host'] : "localhost";
		$this->port = isset($value['parameters']['hypertable_port']) ? $value['parameters']['hypertable_port'] : 38080; 
		$this->ns_name = isset($value['parameters']['hypertable_ns']) ? $value['parameters']['hypertable_ns'] : "maia"; 

		try {
			parent::__construct($this->host, $this->port);
			$this->ns = $this->namespace_open($this->ns_name);
		}
		catch (TTransportException $e)
		{
			throw new HTDriverException($e);
		}
		catch (TException $e)
		{
			throw new HTDriverException($e);
		}
	}
	
	
	/**
	* Get instance of htdriver
	*/
	static public function getInstance()
	{
		if (!self::$instance)
			self::$instance = new HTDriver;
		return self::$instance;
	}
	
	/**
	* execute hql query
	*/
	public function query($hql)
	{
		return $this->hql_query($this->ns, $hql);
	}
	
	/**
	* Destructor
	*/
	public function __destruct()
	{
		$this->namespace_close($this->ns);
	}

	/**
	* Select
	*/
	public function select()
	{
		return Select::getInstance();
	}
	
	/**
	* Insert
	*/
	public function insert()
	{
		return Insert::getInstance();
	}
	
	/**
	* Select
	*/
	public function delete()
	{
		return Delete::getInstance();
	}
}

?>
