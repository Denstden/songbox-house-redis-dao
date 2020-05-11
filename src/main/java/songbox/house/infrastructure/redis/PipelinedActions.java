package songbox.house.infrastructure.redis;

import redis.clients.jedis.Pipeline;

public interface PipelinedActions {
    void perform(Pipeline pipeline);
}
