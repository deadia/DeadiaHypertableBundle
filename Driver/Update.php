<?php

namespace Deadia\Driver;
/**
* Hypertable Update/Insert Driver
*/
final class Update extends Driver {
	/**
	* HypertableModify instance for singleton
	*/
	static private $instance = NULL;
	
	/**
	* Hypertable mutator
	*/
	private $mutator;
	
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
	* Set cell
	*/
	public function setCell($row, $column, $value)
	{
		$key = new Hypertable_ThriftGen_Key(
											array(
											'row'=> $row,
											'column_family'=> $column, 
											'flag' => 2
											)
										);
		$this->cells[] = new Hypertable_ThriftGen_Cell(
																array(
																'key' => $key, 
																'value'=> $value
																)
		);
		return $this;
	}
	
	/**
	* Set cell with qualifier
	*/
	public function setCellWithQualifier($row, $column, $qualifier, $value)
	{
		$key = new Hypertable_ThriftGen_Key(
											array(
											'row'=> $row,
											'column_family'=> $column, 
											'column_qualifier' => $qualifier,
											'flag' => 2
											)
										);
		$this->cells[] = new Hypertable_ThriftGen_Cell(
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
		$this->mutator = $this->mutator_open($this->ns, $this->table, NULL, 0);
		$insert = $this->mutator_set_cells($this->mutator, $this->cells); 
		$this->mutator_flush($this->mutator);
		$this->mutator_close($this->mutator);
		$this->table = NULL;
		$this->cells = array();
		return $insert;
	}
	
	/**
	* Singleton get instance of HTDriverModify
	*/
	static public function getInstance()
	{
		if (!self::$instance)
		self::$instance = new Update();
		return self::$instance;
	}
}

?>