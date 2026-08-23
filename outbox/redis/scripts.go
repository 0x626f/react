package redis

const luaTypeGuard = `
local expected = {'hash','hash','zset','zset','zset','zset','zset','zset','zset','zset','zset','zset','zset','zset','zset'}
for i=1,#expected do
  local actual = redis.call('TYPE', KEYS[i])['ok']
  if actual ~= 'none' and actual ~= expected[i] then
    return redis.error_reply('REACT_OUTBOX_WRONGTYPE_' .. tostring(i))
  end
end
`

const luaTime = `
local function server_now_us()
  local value = redis.call('TIME')
  return tonumber(value[1]) * 1000000 + tonumber(value[2])
end
local function destination_member(record, available)
  return record.destination_encoded .. '|' .. string.format('%020.0f', available) .. '|' .. record.id
end
local function encode_us(value)
  return string.format('%.0f', value)
end
`

var appendScript = newLuaScript(`-- react-outbox:v1:append
` + luaTypeGuard + `
local DOMAIN_KEY_OFFSET = 15
local DOMAIN_ARG_OFFSET = 2
-- react-outbox:domain-validation-hook
local mode = tonumber(ARGV[1])
local batch = cjson.decode(ARGV[2])
local results = {}
local inserts = {}
local seen_id = {}
local seen_key = {}

for i, candidate in ipairs(batch) do
  local raw_id = redis.call('HGET', KEYS[1], candidate.id)
  local key_id = false
  local raw_key = false
  if candidate.idempotency_key ~= '' then
    key_id = redis.call('HGET', KEYS[2], candidate.idempotency_key)
    if key_id then raw_key = redis.call('HGET', KEYS[1], key_id) end
  end
  if key_id and not raw_key then return {-3} end
  if raw_id and raw_key and candidate.id ~= key_id then return {-3} end

  local existing = raw_id or raw_key
  if not existing then
    local planned = seen_id[candidate.id]
    if candidate.idempotency_key ~= '' and seen_key[candidate.idempotency_key] then
      if planned and planned.id ~= seen_key[candidate.idempotency_key].id then return {-3} end
      planned = seen_key[candidate.idempotency_key]
    end
    if planned then existing = planned.raw end
  end

  if existing then
    if mode == 0 then return {-2} end
    local previous = cjson.decode(existing)
    if previous.content_digest ~= candidate.content_digest then return {-3} end
    results[i] = existing
  else
    local raw = cjson.encode(candidate)
    local planned = {id=candidate.id, raw=raw, value=candidate}
    seen_id[candidate.id] = planned
    if candidate.idempotency_key ~= '' then seen_key[candidate.idempotency_key] = planned end
    table.insert(inserts, planned)
    results[i] = raw
  end
end

-- react-outbox:domain-apply-hook
for _, planned in ipairs(inserts) do
  local record = planned.value
  redis.call('HSET', KEYS[1], record.id, planned.raw)
  if record.idempotency_key ~= '' then redis.call('HSET', KEYS[2], record.idempotency_key, record.id) end
  redis.call('ZADD', KEYS[3], record.available_at_us, record.id)
  redis.call('ZADD', KEYS[8], 0, record.query_member)
  redis.call('ZADD', KEYS[9], 0, record.query_member)
  redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
  redis.call('ZADD', KEYS[15], 0, record.destination_query_member)
end
local response = {0}
for i=1,#results do table.insert(response, results[i]) end
return response
`)

var claimScript = newLuaScript(`-- react-outbox:v1:claim
` + luaTypeGuard + luaTime + `
local owner = ARGV[1]
local limit = tonumber(ARGV[2])
local lease_duration = tonumber(ARGV[3])
local recovery_limit = tonumber(ARGV[4])
local tokens = cjson.decode(ARGV[5])
local destinations = cjson.decode(ARGV[6])
local max_response_bytes = tonumber(ARGV[7])
local now = server_now_us()
if now + lease_duration > 9007199254740991 then return {-5} end
local destination_set = {}
for _, destination in ipairs(destinations) do destination_set[destination] = true end

local expired = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, recovery_limit)
for _, id in ipairs(expired) do
	local raw = redis.call('HGET', KEYS[1], id)
  if not raw then
    redis.call('ZREM', KEYS[4], id)
  else
    local record = cjson.decode(raw)
    if record.state == 'leased' and tonumber(record.lease_until_us) <= now then
      redis.call('ZREM', KEYS[4], id)
      redis.call('ZREM', KEYS[10], record.query_member)
      redis.call('ZREM', KEYS[14], record.pending_destination_member)
      record.lease_owner = ''
      record.lease_token = ''
      record.lease_until_us = encode_us(0)
	  record.updated_at_us = encode_us(now)
      record.version = tonumber(record.version) + 1
      if tonumber(record.attempts) >= tonumber(record.max_attempts) then
        record.state = 'dead'
		record.dead_at_us = encode_us(now)
        record.last_error_code = 'lease_expired_exhausted'
        record.last_error_message = 'lease expired after the final delivery attempt'
        redis.call('ZADD', KEYS[6], now, id)
        redis.call('ZADD', KEYS[12], 0, record.query_member)
      else
        record.state = 'pending'
		record.available_at_us = encode_us(now)
        record.last_error_code = 'lease_expired'
        record.last_error_message = 'delivery lease expired'
        redis.call('ZADD', KEYS[3], now, id)
        redis.call('ZADD', KEYS[9], 0, record.query_member)
        record.pending_destination_member = destination_member(record, now)
        redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
      end
      redis.call('HSET', KEYS[1], id, cjson.encode(record))
	else
	  if record.state == 'leased' then
		redis.call('ZADD', KEYS[4], record.lease_until_us, id)
	  else
		redis.call('ZREM', KEYS[4], id)
		redis.call('ZREM', KEYS[10], record.query_member)
	  end
	end
  end
end

local due = {}
if #destinations == 0 then
  due = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, limit)
else
  local candidates = {}
  local seen = {}
  local upper_time = string.format('%020.0f', now)
  for _, destination in ipairs(destinations) do
	local members = redis.call('ZRANGEBYLEX', KEYS[14], '[' .. destination .. '|', '[' .. destination .. '|' .. upper_time .. '|~', 'LIMIT', 0, limit)
	for _, member in ipairs(members) do
	  local tail = string.sub(member, string.len(destination) + 2)
	  local available_text, id = string.match(tail, '^(%d+)|(.+)$')
	  if id and not seen[id] then
		table.insert(candidates, {id=id, available=tonumber(available_text)})
		seen[id] = true
	  end
	end
  end
  table.sort(candidates, function(a,b)
    if a.available ~= b.available then return a.available < b.available end
	return a.id < b.id
  end)
  for i=1,math.min(limit,#candidates) do table.insert(due,candidates[i].id) end
end
local response = {0}
local token_index = 1
local response_bytes = 0
for _, id in ipairs(due) do
  if #response > limit then break end
  local raw = redis.call('HGET', KEYS[1], id)
  if not raw then
    redis.call('ZREM', KEYS[3], id)
  else
	  local record = cjson.decode(raw)
	  local destination_allowed = #destinations == 0 or destination_set[record.destination_encoded] == true
	  if destination_allowed and record.state == 'pending' and tonumber(record.available_at_us) <= now and tonumber(record.attempts) < tonumber(record.max_attempts) then
      record.state = 'leased'
      record.attempts = tonumber(record.attempts) + 1
      record.version = tonumber(record.version) + 1
      record.lease_owner = owner
      record.lease_token = tokens[token_index]
	  record.lease_until_us = encode_us(now + lease_duration)
	  record.updated_at_us = encode_us(now)
      record.completed_owner = ''
	  record.completed_token = ''
	  record.completed_version = 0
	  token_index = token_index + 1
	  local encoded = cjson.encode(record)
	  if response_bytes + string.len(encoded) > max_response_bytes then break end
	  response_bytes = response_bytes + string.len(encoded)
	  redis.call('ZREM', KEYS[3], id)
      redis.call('ZREM', KEYS[9], record.query_member)
      redis.call('ZREM', KEYS[14], record.pending_destination_member)
      redis.call('ZADD', KEYS[4], record.lease_until_us, id)
      redis.call('ZADD', KEYS[10], 0, record.query_member)
	  redis.call('HSET', KEYS[1], id, encoded)
      table.insert(response, encoded)
	else
	  if record.state == 'pending' and tonumber(record.attempts) < tonumber(record.max_attempts) then
		redis.call('ZADD', KEYS[3], record.available_at_us, id)
		record.pending_destination_member = destination_member(record, tonumber(record.available_at_us))
		redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
	  else
		redis.call('ZREM', KEYS[3], id)
		redis.call('ZREM', KEYS[9], record.query_member)
		redis.call('ZREM', KEYS[14], record.pending_destination_member)
	  end
	end
  end
end
return response
`)

var renewScript = newLuaScript(`-- react-outbox:v1:renew
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
local now = server_now_us()
if record.state ~= 'leased' or record.lease_owner ~= ARGV[2] or record.lease_token ~= ARGV[3]
  or tonumber(record.version) ~= tonumber(ARGV[4]) or tonumber(record.lease_until_us) <= now then return -2 end
local until_value = tonumber(ARGV[5])
if until_value <= now or until_value > now + tonumber(ARGV[6]) then return -5 end
record.lease_until_us = encode_us(until_value)
record.updated_at_us = encode_us(now)
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
redis.call('ZADD', KEYS[4], until_value, record.id)
return 0
`)

var acknowledgeScript = newLuaScript(`-- react-outbox:v1:acknowledge
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state == 'delivered' then
  if record.completed_owner == ARGV[2] and record.completed_token == ARGV[3] and tonumber(record.completed_version) == tonumber(ARGV[4]) then return 1 end
  return -3
end
local now = server_now_us()
if record.state ~= 'leased' or record.lease_owner ~= ARGV[2] or record.lease_token ~= ARGV[3]
  or tonumber(record.version) ~= tonumber(ARGV[4]) or tonumber(record.lease_until_us) <= now then return -2 end
redis.call('ZREM', KEYS[4], record.id)
redis.call('ZREM', KEYS[10], record.query_member)
record.state = 'delivered'
record.delivered_at_us = encode_us(now)
record.updated_at_us = encode_us(now)
record.completed_owner = record.lease_owner
record.completed_token = record.lease_token
record.completed_version = record.version
record.lease_owner = ''
record.lease_token = ''
record.lease_until_us = encode_us(0)
record.version = tonumber(record.version) + 1
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
redis.call('ZADD', KEYS[5], now, record.id)
redis.call('ZADD', KEYS[11], 0, record.query_member)
return 0
`)

var retryScript = newLuaScript(`-- react-outbox:v1:retry
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
local now = server_now_us()
if record.state ~= 'leased' or record.lease_owner ~= ARGV[2] or record.lease_token ~= ARGV[3]
  or tonumber(record.version) ~= tonumber(ARGV[4]) or tonumber(record.lease_until_us) <= now then return -2 end
redis.call('ZREM', KEYS[4], record.id)
redis.call('ZREM', KEYS[10], record.query_member)
record.lease_owner = ''
record.lease_token = ''
record.lease_until_us = encode_us(0)
record.last_error_code = ARGV[6]
record.last_error_message = ARGV[7]
record.updated_at_us = encode_us(now)
record.version = tonumber(record.version) + 1
if tonumber(record.attempts) >= tonumber(record.max_attempts) then
  record.state = 'dead'
  record.dead_at_us = encode_us(now)
  redis.call('ZADD', KEYS[6], now, record.id)
  redis.call('ZADD', KEYS[12], 0, record.query_member)
else
  local available = tonumber(ARGV[5])
  if available < now then available = now end
  record.state = 'pending'
  record.available_at_us = encode_us(available)
  record.pending_destination_member = destination_member(record, available)
  redis.call('ZADD', KEYS[3], available, record.id)
  redis.call('ZADD', KEYS[9], 0, record.query_member)
  redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
end
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
return 0
`)

var releaseScript = newLuaScript(`-- react-outbox:v1:release
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
local now = server_now_us()
if record.state ~= 'leased' or record.lease_owner ~= ARGV[2] or record.lease_token ~= ARGV[3]
  or tonumber(record.version) ~= tonumber(ARGV[4]) or tonumber(record.lease_until_us) <= now then return -2 end
redis.call('ZREM', KEYS[4], record.id)
redis.call('ZREM', KEYS[10], record.query_member)
record.lease_owner = ''
record.lease_token = ''
record.lease_until_us = encode_us(0)
record.updated_at_us = encode_us(now)
record.version = tonumber(record.version) + 1
if tonumber(record.attempts) >= tonumber(record.max_attempts) then
  record.state = 'dead'
  record.dead_at_us = encode_us(now)
  record.last_error_code = 'released_exhausted'
  record.last_error_message = 'delivery lease released after the final attempt'
  redis.call('ZADD', KEYS[6], now, record.id)
  redis.call('ZADD', KEYS[12], 0, record.query_member)
else
  local available = tonumber(ARGV[5])
  if available < now then available = now end
  record.state = 'pending'
  record.available_at_us = encode_us(available)
  record.pending_destination_member = destination_member(record, available)
  redis.call('ZADD', KEYS[3], available, record.id)
  redis.call('ZADD', KEYS[9], 0, record.query_member)
  redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
end
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
return 0
`)

var deadLetterScript = newLuaScript(`-- react-outbox:v1:dead-letter
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
local now = server_now_us()
if record.state ~= 'leased' or record.lease_owner ~= ARGV[2] or record.lease_token ~= ARGV[3]
  or tonumber(record.version) ~= tonumber(ARGV[4]) or tonumber(record.lease_until_us) <= now then return -2 end
redis.call('ZREM', KEYS[4], record.id)
redis.call('ZREM', KEYS[10], record.query_member)
record.state = 'dead'
record.dead_at_us = encode_us(now)
record.updated_at_us = encode_us(now)
record.last_error_code = ARGV[5]
record.last_error_message = ARGV[6]
record.lease_owner = ''
record.lease_token = ''
record.lease_until_us = encode_us(0)
record.version = tonumber(record.version) + 1
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
redis.call('ZADD', KEYS[6], now, record.id)
redis.call('ZADD', KEYS[12], 0, record.query_member)
return 0
`)

var cancelScript = newLuaScript(`-- react-outbox:v1:cancel
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state == 'cancelled' then return 1 end
if record.state ~= 'pending' then return -4 end
local now = server_now_us()
redis.call('ZREM', KEYS[3], record.id)
redis.call('ZREM', KEYS[9], record.query_member)
redis.call('ZREM', KEYS[14], record.pending_destination_member)
record.state = 'cancelled'
record.cancelled_at_us = encode_us(now)
record.updated_at_us = encode_us(now)
record.last_error_code = 'cancelled'
record.last_error_message = ARGV[2]
record.version = tonumber(record.version) + 1
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
redis.call('ZADD', KEYS[7], now, record.id)
redis.call('ZADD', KEYS[13], 0, record.query_member)
return 0
`)

var rescheduleScript = newLuaScript(`-- react-outbox:v1:reschedule
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state ~= 'pending' then return -4 end
local available = tonumber(ARGV[2])
if tonumber(record.available_at_us) == available then return 1 end
local now = server_now_us()
redis.call('ZREM', KEYS[14], record.pending_destination_member)
record.available_at_us = encode_us(available)
record.pending_destination_member = destination_member(record, available)
record.updated_at_us = encode_us(now)
record.version = tonumber(record.version) + 1
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
redis.call('ZADD', KEYS[3], available, record.id)
redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
return 0
`)

var requeueScript = newLuaScript(`-- react-outbox:v1:requeue
` + luaTypeGuard + luaTime + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state ~= 'dead' then return -4 end
local now = server_now_us()
local available = tonumber(ARGV[2])
if available == 0 then available = now end
local attempts = tonumber(record.attempts)
if tonumber(ARGV[3]) == 1 then attempts = 0 end
local maximum = tonumber(record.max_attempts)
if tonumber(ARGV[4]) > 0 then maximum = tonumber(ARGV[4]) end
if maximum <= attempts then return -5 end
redis.call('ZREM', KEYS[6], record.id)
redis.call('ZREM', KEYS[12], record.query_member)
record.state = 'pending'
record.available_at_us = encode_us(available)
record.pending_destination_member = destination_member(record, available)
record.dead_at_us = encode_us(0)
record.last_error_code = ''
record.last_error_message = ''
record.attempts = attempts
record.max_attempts = maximum
record.updated_at_us = encode_us(now)
record.version = tonumber(record.version) + 1
record.completed_owner = ''
record.completed_token = ''
record.completed_version = 0
redis.call('HSET', KEYS[1], record.id, cjson.encode(record))
redis.call('ZADD', KEYS[3], available, record.id)
redis.call('ZADD', KEYS[9], 0, record.query_member)
redis.call('ZADD', KEYS[14], 0, record.pending_destination_member)
return 0
`)

var purgeScript = newLuaScript(`-- react-outbox:v1:purge
` + luaTypeGuard + `
local states = cjson.decode(ARGV[1])
local cutoff = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local hot = {delivered=KEYS[5], dead=KEYS[6], cancelled=KEYS[7]}
local query = {delivered=KEYS[11], dead=KEYS[12], cancelled=KEYS[13]}
local expected_time = {delivered='delivered_at_us', dead='dead_at_us', cancelled='cancelled_at_us'}
local removed = 0
for _, state in ipairs(states) do
  if removed >= limit then break end
  local ids = redis.call('ZRANGEBYSCORE', hot[state], '-inf', '(' .. tostring(cutoff), 'LIMIT', 0, limit-removed)
  for _, id in ipairs(ids) do
    if removed >= limit then break end
    local raw = redis.call('HGET', KEYS[1], id)
    if not raw then
      redis.call('ZREM', hot[state], id)
    else
      local record = cjson.decode(raw)
      if record.state == state and tonumber(record[expected_time[state]]) < cutoff then
        if record.idempotency_key ~= '' and redis.call('HGET', KEYS[2], record.idempotency_key) == id then
          redis.call('HDEL', KEYS[2], record.idempotency_key)
        end
        redis.call('HDEL', KEYS[1], id)
        redis.call('ZREM', hot[state], id)
        redis.call('ZREM', query[state], record.query_member)
        redis.call('ZREM', KEYS[8], record.query_member)
        redis.call('ZREM', KEYS[14], record.pending_destination_member)
        redis.call('ZREM', KEYS[15], record.destination_query_member)
        redis.call('ZREM', KEYS[3], id)
        redis.call('ZREM', KEYS[4], id)
        removed = removed + 1
      else
        redis.call('ZREM', hot[state], id)
      end
    end
  end
end
return removed
`)
