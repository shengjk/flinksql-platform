package io.github.shengjk.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;

/**
 * @author shengjk1
 * @date 2018/7/16
 */
public class RedisUtil {
	protected final static org.slf4j.Logger logger = LoggerFactory.getLogger(RedisUtil.class);


	/**
	 * @param redisHp
	 * @return
	 */
	public static JedisCluster getJedisCluster(String redisHp) {
		HashSet<HostAndPort> hp = getSetHP(redisHp);
		return new JedisCluster(hp, 2000, 200);
	}

	/**
	 * @param redisHp
	 * @param password
	 * @return
	 */
	public static JedisCluster getJedisCluster(String redisHp, String password) {
		if (password == null || password.trim().isEmpty()) {
			return getJedisCluster(redisHp);
		} else {
			HashSet<HostAndPort> hp = getSetHP(redisHp);
			return new JedisCluster(hp, 5000, 2000, 20, password, new GenericObjectPoolConfig());
		}
	}


	/**
	 * @param redisHp
	 * @param timeout
	 * @param maxAttempts
	 * @return
	 */
	public static JedisCluster getJedisCluster(String redisHp, int timeout, int maxAttempts) {
		HashSet<HostAndPort> hp = getSetHP(redisHp);
		return new JedisCluster(hp, timeout, maxAttempts);
	}


	/**
	 * @param redisHp
	 * @return
	 */
	private static HashSet<HostAndPort> getSetHP(String redisHp) {
		HashSet hp = new HashSet<HostAndPort>();
		String[] split = redisHp.split(",", -1);
		for (String str : split) {
			String[] split1 = str.split(":", -1);
			if (split1.length == 2) {
				hp.add(new HostAndPort(split1[0], Integer.parseInt(split1[1])));
			}
		}
		return hp;
	}


	/**
	 * @param conn
	 */
	public static void closeConn(JedisCluster conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			logger.error("JedisCluster close error {}", e);
		}
	}
}
