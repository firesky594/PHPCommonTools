<?php

declare(strict_types=1);

namespace Firesky594\PhpCommonTools\Redis;

/** Redis原始命令协议；数组按RESP结构返回，服务端错误必须抛异常。 */
interface TransportInterface
{
    /** 执行单条原始命令，不序列化参数、不添加连接级键前缀；无消息返回null或false。 */
    public function execute(array $command): mixed;
}
