<?php

declare(strict_types=1);

namespace Firesky594\PhpCommonTools\Queue;

use Closure;
use InvalidArgumentException;
use Firesky594\PhpCommonTools\Redis\TransportInterface;
use RuntimeException;
use Throwable;

/** 独立的单Redis实例Stream队列；每个Stream只允许一个逻辑消费组，业务需保证幂等。 */
final class StreamQueue
{
    private Closure $failureHandler;

    /** 注入已有Redis连接与可靠失败保存回调；回调须返回true才确认终态消息。 */
    public function __construct(
        private TransportInterface $redis,
        callable $failureHandler,
        private string $prefix = 'wf-pct:',
        private array $retryDelays = [2, 10, 30, 60]
    ) {
        if ($prefix === '' || !$retryDelays || !array_is_list($retryDelays)) {
            throw new InvalidArgumentException('Prefix and retry delays are required');
        }
        foreach ($retryDelays as $delay) {
            if (!is_int($delay) || $delay < 1) {
                throw new InvalidArgumentException('Retry delays must be positive integers');
            }
        }
        $this->failureHandler = Closure::fromCallable($failureHandler);
    }

    /** 校验队列名并生成独立键，重试集合与业务Stream分离。 */
    public function streamKey(string $queue): string
    {
        if (!preg_match('/^[a-zA-Z0-9_-]+$/D', $queue)) {
            throw new InvalidArgumentException('Invalid queue name');
        }
        return $this->prefix . 'stream:' . $queue;
    }

    /** 返回当前实例的延迟任务集合键，便于只读监控和容量检查。 */
    public function retryKey(): string
    {
        return $this->prefix . 'retry';
    }

    /** 投递独立信封，业务数据与重试字段隔离；返回Stream消息ID。 */
    public function publish(string $queue, array $payload): string
    {
        $envelope = ['id' => bin2hex(random_bytes(16)), 'attempt' => 0, 'payload' => $payload];
        $id = $this->redis->execute(['XADD', $this->streamKey($queue), '*', 'data', $this->encode($envelope)]);
        if (!is_string($id)) {
            throw new RuntimeException('Stream publish failed');
        }
        return $id;
    }

    /** 创建消费组，从最早消息开始；组名绑定为队列名以避免误建多个逻辑组。 */
    public function ensureGroup(string $queue): void
    {
        $script = <<<'LUA'
local result = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0', 'MKSTREAM')
if type(result) == 'table' and result.err then
    if string.find(result.err, 'BUSYGROUP', 1, true) then return 0 end
    return redis.error_reply(result.err)
end
return 1
LUA;
        $this->evaluate($script, [$this->streamKey($queue), $queue], 1);
    }

    /** 优先恢复固定consumer自己的Pending，再读取一条新消息；异常保持Pending并交调用者处理。 */
    public function consumeOnce(string $queue, string $consumer, callable $handler, int $blockMs = 800): bool
    {
        if ($consumer === '' || $blockMs < 0) {
            throw new InvalidArgumentException('Consumer and nonnegative block time required');
        }
        $this->ensureGroup($queue);
        $key = $this->streamKey($queue);
        $rows = $this->redis->execute(['XREADGROUP', 'GROUP', $queue, $consumer, 'COUNT', 1,
            'STREAMS', $key, '0']);
        if (empty($rows[0][1])) {
            $command = ['XREADGROUP', 'GROUP', $queue, $consumer, 'COUNT', 1];
            if ($blockMs > 0) {
                array_push($command, 'BLOCK', $blockMs);
            }
            array_push($command, 'STREAMS', $key, '>');
            $rows = $this->redis->execute($command);
        }
        foreach ($rows[0][1] ?? [] as $row) {
            $fields = $row[1] ?? [];
            $data = '';
            for ($i = 0; $i < count($fields); $i += 2) {
                if ($fields[$i] === 'data') {
                    $data = $fields[$i + 1] ?? '';
                    break;
                }
            }
            $this->process($queue, (string)$row[0], $data, $handler);
            return true;
        }
        return false;
    }

    /** 常驻消费，shouldStop由调用方提供；连接异常向上抛出，不吞掉基础设施错误。 */
    public function consume(string $queue, string $consumer, callable $handler, callable $shouldStop): void
    {
        while (!$shouldStop()) {
            $this->consumeOnce($queue, $consumer, $handler);
        }
    }

    /** 校验信封并执行回调；code=1成功，terminal=true终止，其余按统一退避重试。 */
    private function process(string $queue, string $id, string $raw, callable $handler): void
    {
        try {
            $envelope = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            if (
                !is_array($envelope) || !is_string($envelope['id'] ?? null)
                || !is_int($envelope['attempt'] ?? null) || $envelope['attempt'] < 0
                || !is_array($envelope['payload'] ?? null)
            ) {
                throw new InvalidArgumentException('Invalid message envelope');
            }
        } catch (Throwable $e) {
            $this->finishFailure(
                $queue,
                $id,
                ['raw_base64' => base64_encode($raw)],
                ['code' => 0, 'terminal' => true, 'msg' => 'Invalid message envelope']
            );
            return;
        }
        try {
            $result = $handler($envelope['payload']);
        } catch (Throwable $e) {
            $result = ['code' => 0, 'msg' => 'Handler failed', 'exception' => get_class($e)];
        }
        if (!is_array($result) || !array_key_exists('code', $result)) {
            $result = ['code' => 0, 'msg' => 'Handler must return code'];
        }
        if (in_array($result['code'], [1, '1'], true)) {
            $this->ack($queue, $id);
            return;
        }
        $attempt = $envelope['attempt'];
        if (!empty($result['terminal']) || $attempt >= count($this->retryDelays)) {
            $result['retry_exhausted'] = $attempt >= count($this->retryDelays);
            $this->finishFailure($queue, $id, $envelope, $result);
            return;
        }
        $envelope['attempt']++;
        $task = $this->encode(['queue' => $queue, 'envelope' => $envelope, 'source' => $id]);
        $script = <<<'LUA'
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return 0 end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
LUA;
        $this->evaluate($script, [
            $this->streamKey($queue),
            $this->retryKey(),
            $queue,
            $id,
            time() + $this->retryDelays[$attempt],
            $task
        ], 2);
    }

    /** 先可靠保存失败再确认；失败保存可能重复执行，回调应使用信封ID幂等。 */
    private function finishFailure(string $queue, string $id, array $envelope, array $result): void
    {
        if (($this->failureHandler)($queue, $id, $envelope, $result) !== true) {
            throw new RuntimeException('Failure handler did not persist the failure');
        }
        $this->ack($queue, $id);
    }

    /** 原子确认并删除消息；仅适用于当前包单逻辑组的Stream。 */
    private function ack(string $queue, string $id): void
    {
        $this->evaluate(
            "redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); return redis.call('XDEL', KEYS[1], ARGV[2])",
            [$this->streamKey($queue), $queue, $id],
            1
        );
    }

    /** 有限投递到期重试任务；Lua先投递再移除，多调度器不会重复搬运同一成员。 */
    public function dispatchDue(int $limit = 100, ?int $now = null): int
    {
        if ($limit < 1 || $limit > 10000) {
            throw new InvalidArgumentException('Dispatch limit must be between 1 and 10000');
        }
        $now ??= time();
        $tasks = $this->redis->execute(['ZRANGEBYSCORE', $this->retryKey(), '-inf', (string)$now, 'LIMIT', 0, $limit]);
        if ($tasks === false) {
            throw new RuntimeException('Retry collection read failed');
        }
        $count = 0;
        foreach ($tasks as $member) {
            // 数据损坏抛出并保留成员，交由调用方报警和人工处理，不静默丢弃。
            $task = json_decode($member, true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($task) || !is_string($task['queue'] ?? null) || !is_array($task['envelope'] ?? null)) {
                throw new RuntimeException('Invalid retry member');
            }
            $script = <<<'LUA'
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then return 0 end
redis.call('XADD', KEYS[2], '*', 'data', ARGV[3])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
LUA;
            $count += (int)$this->evaluate($script, [
                $this->retryKey(),
                $this->streamKey($task['queue']),
                $member,
                $now,
                $this->encode($task['envelope'])
            ], 2);
        }
        return $count;
    }

    /** 统一编码，JSON异常向上抛出，禁止无效数据静默入队。 */
    private function encode(array $value): string
    {
        return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
    }

    /** 检查Lua执行结果，基础设施错误不得当作确认成功。 */
    private function evaluate(string $script, array $arguments, int $keyCount): mixed
    {
        $result = $this->redis->execute(array_merge(['EVAL', $script, $keyCount], $arguments));
        if ($result === false) {
            throw new RuntimeException('Redis queue script failed');
        }
        return $result;
    }
}
