package songbox.house.infrastructure.redis;

import redis.clients.jedis.Pipeline;

public interface PipelinedActionsResult<R> {
    R perform(Pipeline pipeline);
}
