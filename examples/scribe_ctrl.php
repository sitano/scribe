<?php 
if ($argc < 4) {
	echo $argv[0] . " host port command [command ...]\n" .
	"commands:\n" .
	"name\n" .
	"version\n" .
	"status\n" .
	"alivesince\n" .
	"reinitialize\n" .
	"shutdown\n" .
	"counters\n";
	die();
}

$host = $argv[1];
$port = $argv[2];
$commands = array_slice($argv, 3);

$GLOBALS['THRIFT_ROOT'] = '/home/goir/scribe/thrift-0.8.0/lib/php/src';
require_once $GLOBALS['THRIFT_ROOT'] .'/packages/fb303/FacebookService.php';
require_once $GLOBALS['THRIFT_ROOT'] .'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'] .'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'] .'/protocol/TBinaryProtocol.php';


$sock = new TSocket($host, (int)$port);
$trans = new TFramedTransport($sock);
$prot = new TBinaryProtocol($trans);
$client = new FacebookServiceClient($prot, $prot);
$trans->open();

//  public function getName();
//  public function getVersion();
//  public function getStatus();
//  public function getStatusDetails();
//  public function getCounters();
//  public function getCounter($key);
//  public function setOption($key, $value);
//  public function getOption($key);
//  public function getOptions();
//  public function getCpuProfile($profileDurationInSec);
//  public function aliveSince();
//  public function reinitialize();
//  public function shutdown();

foreach ($commands as $command) {
	switch ($command) {
		case 'name':
			echo $client->getName();
			break;
		case 'version':
			echo $client->getVersion();
			break;
		case 'status':
			$status = array_flip($GLOBALS['E_fb_status']);
			echo $status[$client->getStatus()];
			break;
		case 'statusdetails':
			echo $client->getStatusDetails($a);
			break;
		case 'counters':
			$counters = $client->getCounters();
			foreach ($counters as $name => $count) {
				echo $name . ': ' . $count ."\n";
			}
			break;
		case 'options':
			var_dump($client->getOptions());
			break;
		case 'alivesince':
			$time = $client->aliveSince();
			echo 'timestamp: ' . $time . "\n";
			echo 'datetime: ' . date('Y-m-d H:i:s', $time);
			break;
		case 'reinitialize':
			echo $client->reinitialize();
			break;
		case 'shutdown':
			echo $client->shutdown();
			break;
		default:
			echo 'Unknown command ' . $command;
	}
	echo "\n";
}

/*
	sock = TSocket.TSocket('localhost', port)

    # use input transport factory if provided
    if (trans_factory is None):
        trans = TTransport.TBufferedTransport(sock)
    else:
        trans = trans_factory.getTransport(sock)

    # use input protocol factory if provided
    if (prot_factory is None):
        prot = TBinaryProtocol.TBinaryProtocol(trans)
    else:
        prot = prot_factory.getProtocol(trans)

    # initialize client and open transport
    fb303_client = FacebookService.Client(prot, prot)
    trans.open()

    if (command == 'reload'):
        fb303_client.reinitialize()
*/
        