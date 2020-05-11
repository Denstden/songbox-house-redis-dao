package songbox.house.infrastructure.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.Pool;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

public class RedisDAOImpl implements RedisDAO {

    private final Pool<Jedis> pool;

    public RedisDAOImpl(String host, Integer port, String password, Integer dbIndex, Integer timeoutMs) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        this.pool = new JedisPool(jedisPoolConfig, host, port, timeoutMs, password, dbIndex);
    }

    @Override
    public byte[] get(byte[] key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.get(key);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public String get(String key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.get(key);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long getLong(String key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return Long.valueOf(connection.get(key));
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public String getSet(String key, String value) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.getSet(key, value);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.getSet(key, value);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public String set(String key, String value, SetParams setParams) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.set(key, value, setParams);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public String set(byte[] key, byte[] value, SetParams setParams) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.set(key, value, setParams);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long expire(String key, int seconds) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.expire(key, seconds);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.expire(key, seconds);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long expire(String key, int amount, TimeUnit timeUnit) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.expire(key, (int) timeUnit.toSeconds(amount));
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long expire(byte[] key, int amount, TimeUnit timeUnit) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.expire(key, (int) timeUnit.toSeconds(amount));
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hgetAll(key);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hgetAll(key);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public String hget(String key, String field) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hget(key, field);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Map<String, String> hget(String key, Set<String> fields) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hgetAll(key).entrySet().stream()
                    .filter(entry -> fields.contains(entry.getKey()))
                    .collect(toMap(Entry::getKey, Entry::getValue));
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Map<byte[], byte[]> hget(byte[] key, Set<byte[]> fields) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hgetAll(key).entrySet().stream()
                    .filter(entry -> fields.contains(entry.getKey()))
                    .collect(toMap(Entry::getKey, Entry::getValue));
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hget(key, field);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long hset(String key, String field, String value) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hset(key, field, value);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void hset(String key, Map<String, String> fieldsWithValues) {
        doInPipeline(pipeline -> {
            fieldsWithValues.forEach((k, v) -> pipeline.hset(key, k, v));
        });
    }

    @Override
    public void hset(byte[] key, Map<byte[], byte[]> fieldsWithValues) {
        doInPipeline(pipeline -> {
            fieldsWithValues.forEach((k, v) -> pipeline.hset(key, k, v));
        });
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hset(key, field, value);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Long hdel(String key, Set<String> fields) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.hdel(key, fields.toArray(new String[0]));
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public Set<String> keys(String pattern) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.keys(pattern);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public boolean exists(String key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.exists(key);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public boolean exists(byte[] key) {
        Jedis connection = null;
        try {
            connection = getConnection();
            return connection.exists(key);
        } catch (JedisConnectionException e) {
            close(connection);
            connection = null;
            throw new RedisException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void doInPipeline(PipelinedActions pipelinedActions) {
        Jedis conn = null;
        try {
            conn = getConnection();
            Pipeline pipeline = conn.pipelined();
            pipelinedActions.perform(pipeline);
            pipeline.sync();
        } catch (JedisConnectionException e) {
            close(conn);
            conn = null;
            throw new RedisException(e);
        } finally {
            close(conn);
        }
    }

    @Override
    public <R> R doInPipeline(PipelinedActionsResult<Response<R>> pipelinedActions) {
        Jedis conn = null;
        try {
            conn = getConnection();
            Pipeline pipeline = conn.pipelined();
            Response<R> r = pipelinedActions.perform(pipeline);
            pipeline.sync();
            return r.get();
        } catch (JedisConnectionException e) {
            close(conn);
            conn = null;
            throw new RedisException(e);
        } finally {
            close(conn);
        }
    }

    private Jedis getConnection() {
        return pool.getResource();
    }

    private void close(Jedis connection) {
        ofNullable(connection).ifPresent(Jedis::close);
    }
}
