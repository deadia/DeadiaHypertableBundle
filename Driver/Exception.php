<?php

namespace Deadia\Driver;

/**
* HT Exception class
*/
class Exception extends \Exception {
	
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

?>