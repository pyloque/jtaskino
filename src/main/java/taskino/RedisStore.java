package taskino;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisStore {

    private List<JedisPool> pools;

    public RedisStore() {
        this(URI.create("redis://localhost:6379/0"));
    }

    public RedisStore(URI... uris) {
        pools = new ArrayList<JedisPool>(uris.length);
        for (URI uri : uris) {
            JedisPool pool = new JedisPool(uri, 5000);
            pools.add(pool);
        }
    }

    public void close() {
        for (JedisPool pool : pools) {
            pool.close();
        }
    }

    public void execute(Consumer<Jedis> func) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int index = random.nextInt(pools.size());
        JedisPool pool = pools.get(index);
        try (Jedis jedis = pool.getResource()) {
            try {
                func.accept(jedis);
            } catch (JedisConnectionException e) {
                func.accept(jedis);
            }
        }
    }

}
