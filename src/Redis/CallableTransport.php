<?php

declare(strict_types=1);

namespace Firesky594\PhpCommonTools\Redis;

use Closure;
use Firesky594\PhpCommonTools\Redis\TransportInterface;

/** 用回调接入其他框架、连接池或客户端，不持有或管理其连接生命周期。 */
class CallableTransport implements TransportInterface
{
    private Closure $executor;

    /** executor接收完整命令数组，负责原始RESP结果及异常契约。 */
    public function __construct(callable $executor)
    {
        $this->executor = Closure::fromCallable($executor);
    }

    /** 将命令原样交给宿主，保留其连接池与协程上下文。 */
    public function execute(array $command): mixed
    {
        return ($this->executor)($command);
    }
}
