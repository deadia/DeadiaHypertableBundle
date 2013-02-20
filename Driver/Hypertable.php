<?php
/*
*
* @author Marc PEPIN
*
*/

 
// thrift path
define('THRIFT_PATH', __DIR__);

// hypertable host
define('HT_HOST', 'localhost');

//hypertable namespace
define('HT_NS', 'maia');

// hypertable port
define('HT_PORT', '38080');

require_once(THRIFT_PATH.'/ThriftClient.php');

/**
* HT Exception class
*/
class HTDriverException extends \Exception {
	
	/**
	* Exception object
	*/
	private $trace;
	
	/**
	* Constructor
	*/
	public function __construct(Exception $trace)
	{
		$this->trace = $trace;
		$this->message = $trace->getMessage();
	} 
	
	/**
	* Return HTDriver message
	*/
	public function getError()
	{
		return $this->trace;
	}
}


/**
* Hypertable Base class
*/
class HTDriver extends \Hypertable_ThriftClient{
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
	* Constructor
	*/
	public function __construct()
	{
		global $conf;
	
		try {
			parent::__construct(HT_HOST, HT_PORT);
			$this->ns = $this->namespace_open(HT_NS);
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

}

/**
* HTDriverCondition 
*/
class HTDriverCondition {

	const ROW_REGEXP = 0;
	
	const ROW_EQUAL = 1;
	
	const VALUE_REGEXP = 2;
	
	const VALUE_EQUAL = 3;
	
}

/**
* Hypertable Select Driver
*/
final class HTDriverSelect extends HTDriver {

	/**
	* HypertableSelect instance for singleton
	*/
	static private $instance = NULL;
	
	/**
	* Columns
	*/
	private $columns = array();
	
	/**
	* Hypertable scanner
	*/
	private $scanner;
	
	/**
	* Hypertable scanner argument
	*/
	private $args =  NULL;
	
	/**
	* Column name (GROUP BY)
	*/
	private $group = NULL;
	
	/**
	* Count query
	*/
	private $enableCount = false;
	
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
	* Set scanspec arguments
	* @args array
	*/
	public function setScanSpec($args)
	{
		$this->args = $args;
		return $this;
	}
	
	/**
	* Set Target column
	*/
	public function setColumn($col)
	{
		$this->columns[] = $col;
		$this->args['columns'][] = $col;
		return $this;
	}
	
	/**
	* Set Target columns
	*/
	public function setColumns(array $cols)
	{
		$this->columns = array_merge($this->columns, $cols);
		foreach ($cols as $col)
		$this->args['columns'][] = $col;
		return $this;
	}
	
	/**
	* Set condition (WHERE)
	*/
	public function condition($type, $value)
	{
		if ($type == HTDriverCondition::ROW_REGEXP)
			$this->args['row_regexp'] = $value;
		else if ($type == HTDriverCondition::VALUE_REGEXP)
			$this->args['value_regexp'] = $value;
		else if ($type == HTDriverCondition::ROW_EQUAL)
		{
			$interval = array("start_row" => $value, "start_inclusive" => true, "end_row" => $value, "end_inclusive" => true);
			$interval = new Hypertable_ThriftGen_RowInterval($interval);
			$this->args['row_intervals'][] = $interval;
		}
		else if ($type == HTDriverCondition::VALUE_EQUAL)
		{
			$interval = array("start_cell" => $value, "start_inclusive" => true, "end_cell" => $value, "end_inclusive" => true);
			$interval = new Hypertable_ThriftGen_CellInterval($interval);
			$this->args['cell_intervals'] = $interval;
		}
		return $this;
	}
	
	/**
	* Set timestamp filter
	*/ 
	public function setTimestamp($ts, $type)
	{
		if ($type == ">")
		 	$this->args['start_time'] = $ts;
		else if ($type == "<")
		 	$this->args['end_time'] = $ts;
		 return $this;
	}
	/**
	* Set result limit
	*/
	public function setLimit($max)
	{
		$this->args['row_limit'] = $max;
		return $this;
	}
	
	/**
	* Set cell offset
	*/
	public function setCelloffset($offset)
	{
		$this->args['cell_offset'] = $offset;
		return $this;
	}
	
	/**
	* Set cell limit
	*/
	public function setCellLimit($max)
	{
		$this->args['cell_limit'] = $max;
		return $this;
	}
	
	/**
	* Set result offset
	*/
	public function setOffset($from)
	{
		$this->args['row_offset'] = $from;
		return $this;
	}
	
	/**
	* Sort result by columns
	*/
	private function sortColumns($data)
	{
		$result = array();
		foreach ($data as $cell)
		{
			$row = $cell->key->row;
			if (!isset($result[$row])) {
				$result[$row] = new \stdClass;
				$result[$row]->row = $cell->key->row;
				foreach ($this->columns as $col) {
					$pos = strpos($col, ':') ;
					$qualifier = ($pos) ? substr($col, $pos + 1) : NULL;
					$col = !$pos ? $col : substr($col, 0, $pos);
					if (!$qualifier)
						$result[$row]->{$col} = NULL;
					else if ($qualifier[0] != '/')
						$result[$row]->{$col}[$qualifier] = NULL;
				}
			}
			$result[$row]->{$cell->key->column_family}[$cell->key->column_qualifier] = $cell->value;
		}
		return $result;
	}
	
	public function setKeyOnly($value)
	{
		$this->args['keys_only'] = $value;
		return $this;
	}
	
	public function setOptimisation($val)
	{
		$this->args['scan_and_filter_rows'] = $val;
		return $this;
	}
	
	public function count()
	{
		$this->args['keys_only'] = true;
		$this->args['scan_and_filter_rows'] = true;
		$this->enableCount = true;
		return $this;
	}
	
	/**
	* execute request
	*/
	public function execute()
	{
		//print_r($this->args);
		$scanSpec = NULL;
		if ($this->args)
			$scanSpec = new Hypertable_ThriftGen_ScanSpec($this->args);
		try
		{
			$cells = $this->get_cells($this->ns, $this->table, $scanSpec);
		}
		catch (Hypertable_ThriftGen_ClientException $e)
		{
			throw new HTDriverException($e);
		}
		
		$result = $this->sortColumns($cells);
		$result = $this->enableCount == true ? count($result) : $result;
		$this->enableCount = false;
		$this->args = NULL;
		return $result;

	}
		
	/**
	* query
	*/	
	public function query($hql)
	{
		$cells = $this->hql_query($this->ns, $hql);
		return $this->sortColumns($cells->cells);
	}
	
	/**
	* Singleton get instance of HTDriverSelect
	*/
	static public function getInstance()
	{
		if (!self::$instance)
		self::$instance = new HTDriverSelect();
		//self::$instance->clean();
		return self::$instance;
	}
}

/**
* Hypertable Update/Insert Driver
*/
final class HTDriverUpdate extends HTDriver {
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
		self::$instance = new HTDriverUpdate();
		return self::$instance;
	}
}

/**
* Hypertable Delete Driver
*/
final class HTDriverDelete extends HtDriver{

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
		self::$instance = new HTDriverDelete();
		return self::$instance;
	}
}


/**
* Hypertable Driver Insert 
*/
class HTDriverInsert extends HTDriver {
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
			self::$instance = new HTDriverInsert();
		return self::$instance;
	}
}

?>


