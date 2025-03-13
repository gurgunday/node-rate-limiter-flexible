const RateLimiterStoreAbstract = require('./RateLimiterStoreAbstract');
const RateLimiterRes = require('./RateLimiterRes');

const getPttlLuaScript = `
local key = KEYS[1]
local ttl = server.call('pttl', key)

if ttl == -1 then
    return nil
end

local consumed = server.call('get', key)

return {consumed, ttl}
`;

const incrTtlLuaScript = `
local forceExpire = ARGV[3] == 'true'

if forceExpire then
    if ARGV[2] > 0 then
        server.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
    else
        server.call('set', KEYS[1], ARGV[1])
    end

    local ttl = server.call('pttl', KEYS[1])

    return {ARGV[1], ttl}
end

server.call('set', KEYS[1], 0, 'EX', ARGV[2], 'NX')
local consumed = server.call('incrby', KEYS[1], ARGV[1])
local ttl = server.call('pttl', KEYS[1])

return {consumed, ttl}
`;

class RateLimiterValkeyGLIDE extends RateLimiterStoreAbstract {
  /**
   *
   * @param {Object} opts
   * Defaults {
   *   ... see other in RateLimiterStoreAbstract
   *
   *   storeClient: ValkeyGLIDEClient
   *   Script: Script
   * }
   */
  constructor(opts) {
    super(opts);
    this.client = opts.storeClient;

    if (!opts.Script) {
      throw new TypeError('Script class from Valkey GLIDE is required');
    }

    this._incrTtlLuaScript = new opts.Script(opts.customIncrTtlLuaScript || incrTtlLuaScript);
    this._getPttlLuaScript = new opts.Script(getPttlLuaScript);
  }

  _getRateLimiterRes(rlKey, changedPoints, result) {
    const [consumed, resTtlMs] = result;

    const res = new RateLimiterRes();
    res.consumedPoints = +consumed;
    res.isFirstInDuration = res.consumedPoints === changedPoints;
    res.remainingPoints = Math.max(this.points - res.consumedPoints, 0);
    res.msBeforeNext = resTtlMs;

    return res;
  }

  _upsert(rlKey, points, msDuration, forceExpire = false) {
    const secDuration = Math.floor(msDuration / 1000);

    return this.client.invokeScript(this._incrTtlLuaScript, {
      keys: [rlKey],
      args: [String(points), String(secDuration), String(forceExpire), String(this.points), String(this.duration)],
    });
  }

  _get(rlKey) {
    return this.client
      .invokeScript(this._getPttlLuaScript, {
        keys: [rlKey],
      }).then((result) => {
        if (!result) {
          return null;
        }

        return result;
      });
  }

  _delete(rlKey) {
    return this.client.del([rlKey]).then(result => result === 1);
  }
}

module.exports = RateLimiterValkeyGLIDE;
