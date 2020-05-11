package songbox.house.infrastructure.redis;

public class RedisException extends RuntimeException {
    public RedisException(Throwable e) {
        super("Redis exception", e);
    }
}
