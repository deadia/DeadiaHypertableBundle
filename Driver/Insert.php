<?php

namespace Deadia\Driver;

/**
* Hypertable Driver Insert 
*/
class Insert extends Driver {
	/**
	* HypertableModify instance
	*/
	static private $instance = NULL;
	
	/**
	* Hypertable mutator
	*/
	private $mutator;
	
	/**
	* Hypertable Mutator spec
	*/
	private $mutateSpec;
	
	/**
	* Hypertable cells
	*/
	private $cells = array();
	
	/**
	* Set table
	* @args = string
	*/
	public function setTable($table)
	{
		$this->table = $table;
		return $this;
	}
	
	/**
	* Set cell with qualifier
	*/
	public function setCellWithQualifier($row, $column, $qualifier, $value)
	{
		$key = new \Hypertable_ThriftGen_Key(
											array(
											'row'=> $row,
											'column_family'=> $column, 
											'column_qualifier' => $qualifier,
											'flag' => 255
											)
										);
		$this->cells[] = new \Hypertable_ThriftGen_Cell(
																array(
																'key' => $key, 
																'value'=> $value
																)
		);
		return $this;
	}
	
	/**
	* Set cell
	*/
	public function setCell($row, $column, $value)
	{
		$key = new \Hypertable_ThriftGen_Key(
											array(
											'row'=> $row,
											'column_family'=> $column, 
											'flag' => 255
											)
		);
		$this->cells[] = new \Hypertable_ThriftGen_Cell(
														array(
														'key' => $key, 
														'value'=> $value
														)
		);
		return $this;
	}
	
	/**
	* Execute request
	* Update/insert all cell in specified table
	*/
	public function execute()
	{
		$insert = $this->offer_cells($this->ns, $this->table, $this->mutateSpec, $this->cells);
		$this->table = NULL;
		$this->cells = array();
		$this->mutateSpec = NULL;
		return $insert;
	}
	
	/**
	* Singleton get instance of HTDriverModify
	*/
	static public function getInstance()
	{
		if (!self::$instance)
			self::$instance = new Insert();
		return self::$instance;
	}
}

?>