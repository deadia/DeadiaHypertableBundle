<?php

namespace Deadia\HypertableBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder()
    {
	$treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('deadia_hypertable');
    }
}

?>