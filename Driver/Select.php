<?php
/*
*
* @author Marc PEPIN
*
*/

namespace Deadia\Driver;

/**
* Hypertable Select Driver
*/
final class Select extends Driver {

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
		if ($type == Condition::ROW_REGEXP)
			$this->args['row_regexp'] = $value;
		else if ($type == Condition::VALUE_REGEXP)
			$this->args['value_regexp'] = $value;
		else if ($type == Condition::ROW_EQUAL)
		{
			$interval = array("start_row" => $value, "start_inclusive" => true, "end_row" => $value, "end_inclusive" => true);
			$interval = new \Hypertable_ThriftGen_RowInterval($interval);
			$this->args['row_intervals'][] = $interval;
		}
		else if ($type == Condition::VALUE_EQUAL)
		{
			$interval = array("start_cell" => $value, "start_inclusive" => true, "end_cell" => $value, "end_inclusive" => true);
			$interval = new \Hypertable_ThriftGen_CellInterval($interval);
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
			if (!empty($cell->key->column_qualifier))
				$result[$row]->{$cell->key->column_family}[$cell->key->column_qualifier] = $cell->value;
			else
				$result[$row]->{$cell->key->column_family} = $cell->value;
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
			$scanSpec = new \Hypertable_ThriftGen_ScanSpec($this->args);
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
		self::$instance = new Select();
		//self::$instance->clean();
		return self::$instance;
	}
}

?>


