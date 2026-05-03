import IORedis from 'ioredis';
import { sendMessageToSentry, sendToSentry } from '@sentry';
import { logger } from '@logger';
import { executeCall, EchoCallPayload } from '../esl/connection';
import {
  recordTriggered,
  recordInitiated,
  recordFailed,
} from '../metrics/reporter';

const { REDIS_URL, ECHO_VOICE_RPS, NODE_ENV, DONT_CALL_FREESWITCH } =
  process.env;

export interface ConcurrencyQueueOptions {
  name: string;
  globalConcurrentLimit: number; // max active calls globally
  redisUrl?: string;
  redis?: IORedis;
  perSecondLimit?: number; // max executes per second globally
}

export interface EnqueueCallOptions {
  accountId: string;
  priority?: number;
  callId: string;
  payload: string; // JSON payload or command parts
}

// Lua script to fairly pick next account and reserve a concurrency slot
// Returns formatted string "accountId|callId|payload" or false
const dequeueReserveScript = `
-- KEYS[1] accounts_zset
-- KEYS[2] external_active_calls_key
-- KEYS[3] per_second_rate_zset
-- ARGV[1] queue_name
-- ARGV[2] global_limit
-- ARGV[3] per_second_limit

local accounts_zset = KEYS[1]
local external_active_calls_key = KEYS[2]
local rate_zset = KEYS[3]
local queue_name = ARGV[1]
local global_limit = tonumber(ARGV[2])
local per_second_limit = tonumber(ARGV[3])

local accounts = redis.call('ZRANGE', accounts_zset, 0, 0)
if #accounts == 0 then
  return false
end
local account_id = accounts[1]

local account_queue_key = queue_name .. ':account:' .. account_id .. ':queue'

-- peek at next call (don't pop yet)
local peeked = redis.call('ZRANGE', account_queue_key, 0, 0)
if not peeked or #peeked == 0 then
  -- empty queue for this account, remove from rotation
  redis.call('ZREM', accounts_zset, account_id)
  return false
end
local raw = peeked[1]
-- raw format: callId(36) + payload
local call_id_raw = string.sub(raw, 1, 36)
local call_id = call_id_raw:match("^%s*(.-)%s*$")  -- trim whitespace
local payload = string.sub(raw, 37)

-- Check external active calls count (managed by different repo)
local external_active = tonumber(redis.call('GET', external_active_calls_key) or '0')
if external_active >= global_limit then
  -- Global limit reached, don't dequeue, return false to retry later
  return false
end

-- Per-second global rate limiter (sliding window using TIME)
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window_start = now_ms - 1000
-- prune old entries
redis.call('ZREMRANGEBYSCORE', rate_zset, 0, window_start)
local current_rate = redis.call('ZCARD', rate_zset)
if current_rate >= per_second_limit then
  return false
end

-- Now actually pop the call since we passed the check
redis.call('ZPOPMIN', account_queue_key, 1)

-- Reserve a rate slot by adding this call into the rate window
redis.call('ZADD', rate_zset, now_ms, now_ms .. '-' .. call_id)

-- Check if account still has more calls, if yes rotate to end, if no remove from rotation
local remaining = redis.call('ZCARD', account_queue_key)
if remaining > 0 then
  -- rotate account to end for fairness
  local last_score = redis.call('ZRANGE', accounts_zset, -1, -1, 'WITHSCORES')
  local max_score = 0
  if #last_score > 0 then
    max_score = tonumber(last_score[2])
  end
  redis.call('ZADD', accounts_zset, max_score + 1, account_id)
else
  -- no more calls, remove from rotation
  redis.call('ZREM', accounts_zset, account_id)
end

-- store mapping for release with TTL (5 minutes = 300 seconds)
redis.call('SET', queue_name .. ':call:' .. call_id .. ':account', account_id, 'EX', 300)

return account_id .. '|' .. call_id .. '|' .. payload
`;

// Lua script to release a concurrency slot
const releaseScript = `
-- ARGV[1] queue_name
-- ARGV[2] call_id
local queue_name = ARGV[1]
local call_id = ARGV[2]
local account_id = redis.call('GET', queue_name .. ':call:' .. call_id .. ':account')
if not account_id then
  return 'NOTFOUND'
end
-- Clean up call mapping
redis.call('DEL', queue_name .. ':call:' .. call_id .. ':account')
return 'RELEASED'
`;

// Lua script to decrement external active calls count without going below zero
const decrementExternalActiveScript = `
-- KEYS[1] external_active_calls_key (echo:active:calls:count)
local key = KEYS[1]
local current = tonumber(redis.call('GET', key) or '0')
if current > 0 then
  return redis.call('DECR', key)
end
return 0
`;

export class ConcurrencyQueue {
  private name: string;
  private redis: IORedis;
  private globalLimit: number;
  private perSecondLimit: number;

  constructor(opts: ConcurrencyQueueOptions) {
    this.name = opts.name;
    this.globalLimit = opts.globalConcurrentLimit;
    this.redis =
      opts.redis ??
      new IORedis(opts.redisUrl ?? REDIS_URL ?? 'redis://127.0.0.1:6379');
    this.perSecondLimit = opts.perSecondLimit ?? Number(ECHO_VOICE_RPS ?? 4);
  }

  private accountsZSetKey() {
    return `${this.name}:accounts:rotation`;
  }
  private accountQueueKey(accountId: string) {
    return `${this.name}:account:${accountId}:queue`;
  }
  private externalActiveCallsKey() {
    return 'echo:active:calls:count';
  }

  private perSecondRateKey() {
    return `${this.name}:rate:ps`;
  }

  getRedis() {
    return this.redis;
  }

  async enqueue(opts: EnqueueCallOptions) {
    const { accountId, callId, payload } = opts;
    const priority = opts.priority ?? Date.now();
    // Format message: 36-char callId + payload (simplified, no unused padding)
    const paddedCallId = callId.padEnd(36, ' ');
    const message = `${paddedCallId}${payload}`;
    const pipe = this.redis.multi();
    pipe.zadd(this.accountQueueKey(accountId), priority, message);
    pipe.zadd(this.accountsZSetKey(), 0, accountId);
    await pipe.exec();
  }

  async dequeueReserve(): Promise<{
    accountId: string;
    callId: string;
    payload: string;
  } | null> {
    const result = await this.redis.eval(
      dequeueReserveScript,
      3,
      this.accountsZSetKey(),
      this.externalActiveCallsKey(),
      this.perSecondRateKey(),
      this.name,
      this.globalLimit.toString(),
      this.perSecondLimit.toString()
    );
    if (!result || result === false) return null;
    const str = String(result);
    const [accountId, callId, payload] = str.split('|');
    return { accountId, callId, payload };
  }

  async release(callId: string): Promise<string> {
    const res = await this.redis.eval(releaseScript, 0, this.name, callId);
    return String(res);
  }

  // Decrement external active calls count safely (never below zero)
  async decrementExternalActiveCount(): Promise<number> {
    const res = await this.redis.eval(
      decrementExternalActiveScript,
      1,
      this.externalActiveCallsKey()
    );
    return Number(res);
  }

  async incrementExternalActiveCount(): Promise<number> {
    const newCount = await this.redis.incr(this.externalActiveCallsKey());
    return newCount;
  }

  async processor({
    accountId,
    callId,
    payload,
  }: {
    accountId: string;
    callId: string;
    payload: string;
  }) {
    logger.info(
      `Processing voice task for accountId: ${accountId}, callId: ${callId}`
    );
    let parsed: EchoCallPayload;
    try {
      parsed = JSON.parse(payload) as EchoCallPayload;
    } catch (error: any) {
      logger.error('Failed to parse voice payload:', error);
      sendToSentry(error, {
        context: 'Voice payload parse',
        accountId,
        callId,
        payload,
        environment: NODE_ENV,
      });
      recordFailed();
      await this.release(callId);
      return;
    }

    try {
      // mark initiated attempt for metrics
      recordInitiated();
      const response = await executeCall(parsed);
      logger.info(`Voice call executed for callId=${callId}`);
      // Increment external active calls count only on successful call initiation
      if (response.success) {
        if (!(DONT_CALL_FREESWITCH === 'true'))
          await this.incrementExternalActiveCount();
      } else {
        logger.error('Voice call execution failed', {
          response,
          callId,
          parsed,
        });
        recordFailed();
      }
      // return { success: true };
    } catch (error: any) {
      logger.error('Voice call execution failed', { error, callId, parsed });
      recordFailed();
      sendToSentry(error, {
        context: 'Voice call execution',
        accountId,
        callId,
        parsed,
        environment: NODE_ENV,
      });
    }

    await this.release(callId);

    // Remove this later because this will be handled by the CDR event in API repo
    // const safetyReleaseMs = Number(
    //   process.env.VOICE_SAFETY_RELEASE_MS ?? 10000
    // );
    // setTimeout(async () => {
    //   await this.decrementExternalActiveCount();
    // }, safetyReleaseMs);
  }

  consume(workers = 1, shouldContinue = true) {
    for (let i = 0; i < workers; i += 1) {
      const loop = async () => {
        try {
          const item = await this.dequeueReserve();
          if (item) {
            // mark triggered dequeue for metrics
            recordTriggered();
            await this.processor(item);
          } else {
            await new Promise((r) => {
              setTimeout(r, 200);
            });
          }
        } catch (e: any) {
          logger.error('ConcurrencyQueue consumer loop error:', e);
          sendToSentry(e as Error, {
            context: 'ConcurrencyQueue consumer loop',
            queueName: this.name,
            worker: i,
            environment: NODE_ENV,
          });
          // backoff
          await new Promise((r) => setTimeout(r, 1000));
        } finally {
          if (shouldContinue) {
            setImmediate(loop);
          }
        }
      };
      loop();
    }
  }
}
