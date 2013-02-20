<?php 

namespace Deadia\HypertableBundle\Driver;

/**
* Hypertable Delete Driver
*/
final class Delete extends Driver{

	/**
	* HypertableDelete instance
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
	* @args string 
	*/
	public function setTable($table)
	{
		$this->table = $table;
		return $this;
	}
	
	/**
	* add row to delete
	*/
	public function setRow($row)
	{
		$this->cells[] = new Hypertable_ThriftGen_Cell(array('key' => new Hypertable_ThriftGen_Key( array('row'=> $row, 'flag' => 0))));
		return $this;
	}
	
	/**
	* add cell to delete
	*/
	public function setCell($row, $column)
	{
		$this->cells[] = new Hypertable_ThriftGen_Cell(array('key' => new Hypertable_ThriftGen_Key( array('row'=> $row, 'column_family'=> $column, 'flag' => 1))));
		return $this;
	}
	
	/**
	* add cell with qualifier to delete
	*/
	public function setCellWithQualifier($row, $column, $qualifier)
	{
		$this->cells[] = new Hypertable_ThriftGen_Cell(array('key' => new Hypertable_ThriftGen_Key( array('column_qualifier' => $qualifier, 'row'=> $row, 'column_family'=> $column, 'flag' => 1))));
		return $this;
	}
	
	/**
	* Delete all cells/rows
	*/
	public function execute()
	{
		$this->mutator = $this->mutator_open($this->ns, $this->table, 0, 0);
		$delete = $this->mutator_set_cells($this->mutator, $this->cells); 
		$this->mutator_flush($this->mutator);
		$this->mutator_close($this->mutator);
		$this->table = NULL;
		$this->cells = array();
		return $delete;
	}
	
	/**
	* Singleton get instance of HTDriverModify
	*/
	static public function getInstance()
	{
		if (!self::$instance)
		self::$instance = new Delete();
		return self::$instance;
	}
}



?>
