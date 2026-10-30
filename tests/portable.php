<?php

declare(strict_types=1);

require __DIR__ . '/bootstrap.php';

use Firesky594\PhpCommonTools\Queue\StreamQueue;
use Firesky594\PhpCommonTools\Redis\CallableTransport;
use Firesky594\PhpCommonTools\Redis\LaravelTransport;

$commands = [];
$queue = new StreamQueue(new CallableTransport(function (array $command) use (&$commands) {
    $commands[] = $command;
    return '100-1';
}), fn () => true);
if ($queue->publish('portable', []) !== '100-1' || $commands[0][0] !== 'XADD') {
    throw new RuntimeException('Portable transport failed');
}
$client = new class {
    public function executeRaw(array $command, &$error): string
    {
        $error = true;
        return 'ERR test failure';
    }
};
$adapter = new LaravelTransport(new class($client) {
    public function __construct(private object $client) {}
    public function client(): object { return $this->client; }
});
try {
    $adapter->execute(['PING']);
    throw new LogicException('Predis error was swallowed');
} catch (RuntimeException $e) {
    if ($e->getMessage() !== 'ERR test failure') {
        throw $e;
    }
}
echo "PASS portable core without Redis extension or framework dependencies\n";
