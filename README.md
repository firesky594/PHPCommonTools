# phpCommonTools

独立 Composer 包：`firesky594/php-common-tools`。远程仓库：https://github.com/firesky594/phpCommonTools

PHP >= 8.1、Redis >= 5.0。核心只依赖 `TransportInterface`，不依赖 ThinkPHP、Laravel、Hyperf 或 ext-redis。PhpRedis适配器才需要ext-redis。当前是单Redis实例实现，不支持Redis Cluster。

本包仅在vendor内开发，未加入Manju的Composer依赖、未改主项目自动加载、未替换现有队列。vendor可能被Composer操作移除，后续应在独立源码仓库开发。发布前还需确认许可证。

## 连接适配

所有适配器统一执行原始Redis命令，返回RESP数组结构，不经过框架队列系统。前缀由本包管理，底层原始命令绕过框架连接级前缀和serializer，请为包配置独立Redis连接及唯一前缀。

### 原生PHP / ThinkPHP / 其他框架

```php
use Firesky594\PhpCommonTools\Redis\PhpRedisTransport;

// $redis为宿主已连接、已认证并选好数据库的原生Redis实例。
$transport = new PhpRedisTransport($redis);
```

### Laravel

```php
use Firesky594\PhpCommonTools\Redis\LaravelTransport;
use Illuminate\Support\Facades\Redis;

$transport = new LaravelTransport(Redis::connection('queue_stream'));
```

接收Laravel连接包装对象，每次通过client()取得客户端；支持原生PhpRedis或Predis的executeRaw协议。自定义Artisan命令负责运行消费及重试循环。此包不是Laravel Queue驱动，不自动接入dispatch、queue:work或Horizon；不对请求生命周期自动做队列worker的状态重置。

### Hyperf

```php
use Firesky594\PhpCommonTools\Redis\HyperfTransport;
use Hyperf\Redis\RedisFactory;

// 在Hyperf自定义进程的handle中，从该进程容器获取代理。
$transport = new HyperfTransport($container->get(RedisFactory::class)->get('queue_stream'));
```

每条命令经Hyperf代理执行，使用框架自己的连接池和协程上下文，不将原生连接保存在共享单例里。独立消费进程和重试进程分别运行；进程启动后创建适配器，避免继承父进程已连接socket。宿主负责协程hook、连接池容量与超时设置，Redis读超时应大于consumeOnce的阻塞等待时间。

### 自定义客户端或框架

```php
use Firesky594\PhpCommonTools\Redis\CallableTransport;

$transport = new CallableTransport(function (array $command) use ($client) {
    return $client->executeRaw($command);
});
```

也可实现TransportInterface。服务端错误必须抛异常；正常XREADGROUP超时返回null/false；原始命令参数不允许再做JSON序列化或二次添加键前缀。框架连接池在回调内部借还连接，不能把借出的连接长期共享。

## 队列调用

通过Composer安装后使用PSR-4自动加载；当前本地测试通过tests/bootstrap.php加载本包，主项目不受影响。

```php
use Firesky594\PhpCommonTools\Queue\StreamQueue;

$queue = new StreamQueue($transport,
    function ($name, $messageId, $envelope, $result) use ($failureRepository) {
        // 实现可靠存储，成功必须返回true；失败返回false或抛异常。
        // 信封id用于业务失败幂等；非法消息使用queue+messageId。
        return $failureRepository->save($name, $messageId, $envelope, $result);
    },
    'myapp:production:',
    [2, 10, 30, 60]
);

$queue->publish('tracking', ['event_id' => 'example']);

// 消费进程：stopRequested由宿主信号或框架生命周期控制。
$queue->consume('tracking', 'worker-1', function (array $payload) {
    return ['code' => 1];
}, function () use (&$stopRequested) { return $stopRequested; });

// 另一个重试进程循环调用；宿主负责空闲等待和停止。
$queue->dispatchDue(100);
```

宿主统一负责日志、信号、守护、连接异常退避、重连及进程退出。调用者可以使用consumeOnce自行整合框架事件循环。业务回调不包含框架类，业务数据库事务由项目控制。

## 消息语义

- 单Stream单逻辑组（组名为队列名）；成功后ACK并删除，不用于多组广播。
- 至少一次处理；业务和最终失败保存回调必须幂等。
- 活动消费者名称唯一且稳定，重启后恢复自己的Pending；暂不接管其他consumer的Pending。
- code=1成功；其他结果重试；terminal=true直接保存失败。默认初次处理加四次重试。
- 失败原子转存ZSET并确认原消息；到期搬运原子投回Stream并移除ZSET成员。
- 终态保存失败保留Pending并抛异常，宿主应告警和退避。损坏的延迟成员保留并抛异常，需人工处理。
- 不裁剪未消费消息，不自动清理历史consumer。
- 使用本包独立信封格式，不能直接读取原Manju Stream、ThinkPHP Queue或旧重试集合。

## 验证范围

```bash
# 不需要Redis扩展或框架：验证核心可移植性和客户端错误映射。
php -n tests/portable.php

# 仅连接/tmp路径下的隔离Redis Unix socket。
QUEUE_TEST_REDIS_SOCKET=/tmp/your-test-redis/redis.sock php tests/integration.php
```

QUEUE_TEST_TRANSPORT可选phpredis、callable、laravel、predis-contract、hyperf-contract。五条路径各通过20项真实Redis断言，验证投递、确认、Pending恢复、重试、失败保存异常和命名空间隔离。

Laravel包装、Predis和Hyperf代理使用接口替身，尚未在完整Laravel/Hyperf服务、真实Predis客户端或Swoole协程环境执行集成测试；不将协议测试等同于框架运行时验证。
