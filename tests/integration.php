<?php

declare(strict_types=1);

use Firesky594\PhpCommonTools\Queue\StreamQueue;

require __DIR__ . '/bootstrap.php';

use Firesky594\PhpCommonTools\Redis\PhpRedisTransport;

$socket = getenv('QUEUE_TEST_REDIS_SOCKET');
if (!$socket || !str_starts_with($socket, '/tmp/')) {
    throw new RuntimeException('Set QUEUE_TEST_REDIS_SOCKET to an isolated /tmp Redis socket');
}
$redis = new Redis();
$redis->connect($socket);
$mode = getenv('QUEUE_TEST_TRANSPORT') ?: 'phpredis';
$transport = match ($mode) {
    'phpredis' => new PhpRedisTransport($redis),
    'callable' => new \Firesky594\PhpCommonTools\Redis\CallableTransport(
        fn (array $command) => (new PhpRedisTransport($redis))->execute($command)),
    'laravel' => new \Firesky594\PhpCommonTools\Redis\LaravelTransport(new class($redis) {
        public function __construct(private Redis $redis) {}
        public function client(): Redis { return $this->redis; }
    }),
    'predis-contract' => new \Firesky594\PhpCommonTools\Redis\LaravelTransport(new class($redis) {
        public function __construct(private Redis $redis) {}
        public function client(): object {
            return new class($this->redis) {
                public function __construct(private Redis $redis) {}
                public function executeRaw(array $command, &$error = null): mixed {
                    $error = false;
                    return (new PhpRedisTransport($this->redis))->execute($command);
                }
            };
        }
    }),
    'hyperf-contract' => new \Firesky594\PhpCommonTools\Redis\HyperfTransport(new class($redis) {
        public function __construct(private Redis $redis) {}
        public function __call(string $name, array $arguments): mixed {
            if ($name !== 'rawCommand') { throw new RuntimeException('Unexpected proxy method'); }
            return (new PhpRedisTransport($this->redis))->execute($arguments);
        }
    }),
    default => throw new InvalidArgumentException('Unknown transport test mode'),
};
$prefix = 'test:' . bin2hex(random_bytes(8)) . ':';
$failures = [];
$canPersist = true;
$queue = new StreamQueue($transport, function ($name, $id, $data, $result) use (&$failures, &$canPersist) {
    if (!$canPersist) {
        return false;
    }
    $failures[] = [$name, $id, $data, $result];
    return true;
}, $prefix, [1, 2]);
$assertions = 0;
$assert = function ($value, $message) use (&$assertions) {
    if (!$value) {
        throw new RuntimeException($message);
    }
    $assertions++;
};
try {
    $assert(!$queue->consumeOnce('tracking', 'worker-1', fn () => ['code' => 1], 0), 'empty queue');
    $queue->publish('tracking', ['id' => 123]);
    $assert($queue->consumeOnce('tracking', 'worker-1', function ($data) use ($assert) {
        $assert($data === ['id' => 123], 'payload preserved');
        return ['code' => 1];
    }, 0), 'message consumed');
    $assert($redis->xLen($queue->streamKey('tracking')) === 0, 'success deletes message');
    $queue->publish('tracking', []);
    $fail = fn () => ['code' => 0, 'msg' => 'retry'];
    $queue->consumeOnce('tracking', 'worker-1', $fail, 0);
    $assert($redis->zCard($queue->retryKey()) === 1, 'failure moved to retry');
    $assert($redis->xLen($queue->streamKey('tracking')) === 0, 'failure original removed');
    $assert($queue->dispatchDue(100, time() - 1) === 0, 'future task not dispatched');
    $assert($queue->dispatchDue(100, time() + 10) === 1, 'due task dispatched');
    $assert($queue->dispatchDue(100, time() + 10) === 0, 'duplicate dispatcher safe');
    $queue->consumeOnce('tracking', 'worker-1', $fail, 0);
    $assert($redis->zCard($queue->retryKey()) === 1, 'second retry scheduled');
    $queue->dispatchDue(100, time() + 10);
    $queue->consumeOnce('tracking', 'worker-1', $fail, 0);
    $assert(count($failures) === 1 && $failures[0][3]['retry_exhausted'] === true, 'exhaustion saved');
    $assert($redis->zCard($queue->retryKey()) === 0, 'exhausted stops retry');
    $queue->publish('tracking', ['id' => 'pending']);
    $redis->xReadGroup('tracking', 'worker-1', [$queue->streamKey('tracking') => '>'], 1);
    $assert($queue->consumeOnce('tracking', 'worker-1', fn () => ['code' => 1], 0), 'restart recovers pending');
    $queue->publish('tracking', []);
    $canPersist = false;
    try {
        $queue->consumeOnce('tracking', 'worker-1', fn () => ['code' => 0, 'terminal' => true], 0);
        $assert(false, 'failure storage must fail');
    } catch (RuntimeException $e) {
        $assert($e->getMessage() === 'Failure handler did not persist the failure', 'storage fails explicitly');
    }
    $assert($redis->xLen($queue->streamKey('tracking')) === 1, 'storage failure retains message');
    $canPersist = true;
    $queue->consumeOnce('tracking', 'worker-1', fn () => ['code' => 0, 'terminal' => true], 0);
    $assert(count($failures) === 2, 'pending terminal replay');
    $redis->xAdd($queue->streamKey('tracking'), '*', ['data' => 'broken']);
    $queue->consumeOnce('tracking', 'worker-1', fn () => throw new RuntimeException('must not run'), 0);
    $assert(count($failures) === 3 && isset($failures[2][2]['raw_base64']), 'malformed message archived');
    $queue->publish('tracking', []);
    $queue->consumeOnce('tracking', 'worker-1', fn () => throw new RuntimeException('secret'), 0);
    $assert($redis->zCard($queue->retryKey()) === 1, 'handler exception schedules retry');
    $other = new StreamQueue($transport, fn () => true, $prefix . 'other:');
    $assert(!$other->consumeOnce('tracking', 'worker-1', fn () => ['code' => 1], 0), 'namespace isolation');
    try {
        $queue->publish('../retry', []);
        $assert(false, 'invalid name must fail');
    } catch (InvalidArgumentException $e) {
        $assert(true, 'invalid name rejected');
    }
    echo "PASS: $assertions assertions against isolated Redis ($mode)\n";
} finally {
    $keys = $redis->keys($prefix . '*');
    if ($keys) {
        $redis->del($keys);
    }
    $redis->close();
}
