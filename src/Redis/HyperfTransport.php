<?php

declare(strict_types=1);

namespace Firesky594\PhpCommonTools\Redis;

use Firesky594\PhpCommonTools\Redis\TransportInterface;
use InvalidArgumentException;
use RuntimeException;

/** Hyperf代理适配，每条命令经框架连接池执行，不抽出原生连接长期占用。 */
final class HyperfTransport implements TransportInterface
{
    /** 注入Hyperf Redis或RedisProxy；实例应在正确的进程和协程上下文中创建。 */
    public function __construct(private object $proxy)
    {
        if (!is_callable([$proxy, 'rawCommand'])) {
            throw new InvalidArgumentException('Hyperf Redis proxy with rawCommand required');
        }
    }

    /** 原始命令经代理执行，保留框架的连接借还和协程隔离。 */
    public function execute(array $command): mixed
    {
        $result = $this->proxy->rawCommand(...$command);
        if ($result === false && strtoupper((string)$command[0]) !== 'XREADGROUP') {
            throw new RuntimeException('Hyperf Redis command failed');
        }
        return $result;
    }
}
