<?php

declare(strict_types=1);

namespace Firesky594\PhpCommonTools\Redis;

use Firesky594\PhpCommonTools\Redis\PhpRedisTransport;
use Firesky594\PhpCommonTools\Redis\TransportInterface;
use InvalidArgumentException;
use Redis;
use RuntimeException;

/** Laravel连接包装适配，支持底层PhpRedis和Predis；不依赖Illuminate包。 */
final class LaravelTransport implements TransportInterface
{
    /** 传入Redis::connection()返回对象；每次执行重新取client，允许宿主替换连接。 */
    public function __construct(private object $connection)
    {
        if (!is_callable([$connection, 'client'])) {
            throw new InvalidArgumentException('Laravel Redis connection with client() required');
        }
    }

    /** 按底层客户端选择原始命令API，不通过Laravel队列或序列化封装。 */
    public function execute(array $command): mixed
    {
        $client = $this->connection->client();
        if ($client instanceof Redis) {
            return (new PhpRedisTransport($client))->execute($command);
        }
        if (is_object($client) && is_callable([$client, 'executeRaw'])) {
            $error = false;
            $result = $client->executeRaw($command, $error);
            if ($error) {
                throw new RuntimeException((string)$result);
            }
            return $result;
        }
        throw new InvalidArgumentException('Only standalone PhpRedis or Predis clients are supported');
    }
}
