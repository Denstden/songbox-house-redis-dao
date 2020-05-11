package songbox.house.infrastructure.redis;

import redis.clients.jedis.Response;
import redis.clients.jedis.params.SetParams;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface RedisDAO {
    byte[] get(byte[] key);

    String get(String key);

    Long getLong(String key);

    String getSet(String key, String value);

    byte[] getSet(byte[] key, byte[] value);

    String set(String key, String value, SetParams setParams);

    String set(byte[] key, byte[] value, SetParams setParams);

    Long expire(String key, int seconds);

    Long expire(byte[] key, int seconds);

    Long expire(String key, int amount, TimeUnit timeUnit);

    Long expire(byte[] key, int amount, TimeUnit timeUnit);

    Map<byte[], byte[]> hgetAll(byte[] key);

    Map<String, String> hgetAll(String key);

    String hget(String key, String field);

    Map<String, String> hget(String key, Set<String> fields);

    Map<byte[], byte[]> hget(byte[] key, Set<byte[]> fields);

    byte[] hget(byte[] key, byte[] field);

    Long hset(String key, String field, String value);

    void hset(String key, Map<String, String> fieldsWithValues);

    void hset(byte[] key, Map<byte[], byte[]> fieldsWithValues);

    Long hset(byte[] key, byte[] field, byte[] value);

    Long hdel(String key, Set<String> fields);

    Set<String> keys(String pattern);

    boolean exists(String key);

    boolean exists(byte[] key);

    void doInPipeline(PipelinedActions pipelinedActions);

    <R> R doInPipeline(PipelinedActionsResult<Response<R>> pipelinedActions);
}
