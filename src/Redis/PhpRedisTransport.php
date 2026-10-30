<?php

declare(strict_types=1);

namespace Firesky594\PhpCommonTools\Redis;

use Firesky594\PhpCommonTools\Redis\TransportInterface;
use Redis;
use RuntimeException;

/** 原生PhpRedis适配器；宿主负责连接、认证、数据库和重连。 */
final class PhpRedisTransport implements TransportInterface
{
    /** 注入独立已连接客户端；阻塞消费连接不得跨协程共享。 */
    public function __construct(private Redis $redis)
    {
    }

    /** 原始命令绕过serializer与键前缀；清理旧错误以区分空读取和服务端错误。 */
    public function execute(array $command): mixed
    {
        $this->redis->clearLastError();
        $result = $this->redis->rawCommand(...$command);
        if ($result === false && ($error = $this->redis->getLastError())) {
            throw new RuntimeException($error);
        }
        return $result;
    }
}
