package com.github.zhufg.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * increBy依赖jedis
 * 此工具产生的主要目的是解决缓存击穿和缓存穿透
 * 后期增加了部分内容
 *
 */
public class RedisUtil {
    public static final String EMPTY_STRING="!&*!{}";
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisUtil.class);
    private static final int WAIT_TIME_OUT_SEC=5;

    private static final  StringBuilder LUA_INCR = new StringBuilder();
    static {
        LUA_INCR.append(" local expire_time = ARGV[1]");
        LUA_INCR.append(" local added = redis.call('INCR', KEYS[1])");
        LUA_INCR.append(" if added == 1 then");
        LUA_INCR.append(" redis.call('EXPIRE', KEYS[1], expire_time)");
        LUA_INCR.append(" end");
        LUA_INCR.append(" return added");
    }

    /**
     * maxWaitTime 最长等待时间
     * 为了防止过多线程等待的问题，设置最长等待时间
     * 执行时间不受限，请自行解决执行过慢的问题
     * 不希望通过工具去限制最长执行时间
     * @param key
     * @param type
     * @param sp
     * @param expireTime
     * @param timeUnit
     * @param redisTemplate
     * @param maxWaitTime 最长等待时间
     * @param <T>
     * @return
     * @throws TimeoutException
     */
    public static <T>List getListCache(String key, Type type, Supplier<List<T>> sp, Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate, int maxWaitTime) throws TimeoutException {
        List<T> t = null;
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setExpireTimeMs(TimeUnit.MILLISECONDS.convert(expireTime, timeUnit));
        cacheConfig.setMaxWaitTimeMs(TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS));

        return getListCache(key, type, sp, redisTemplate, cacheConfig);
    }
    public static <T>List getListCache(String key, Type type, Supplier<List<T>> sp, RedisTemplate redisTemplate,CacheConfig cacheConfig) throws TimeoutException {
        List<T> t = null;

        String value = lockCacheGet(key, sp, redisTemplate,cacheConfig);
        if (value == null) {
            return Collections.EMPTY_LIST;
        }
        t = JSON.parseObject(value, new TypeReference<List<T>>(type) {});

        return t;
    }

    /**
     * maxWaitTime 默认5秒
     * 为了防止过多线程等待的问题，设置最长等待时间
     * 执行时间不受限，请自行解决执行过慢的问题
     * 不希望通过工具去限制最长执行时间
     * @param key
     * @param type
     * @param sp
     * @param expireTime
     * @param timeUnit
     * @param redisTemplate
     * @param <T>
     * @return
     * @throws TimeoutException
     */
    public static <T>List getListCache(String key,Type type, Supplier<List<T>> sp, Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate) throws TimeoutException {
        return getListCache(key, type,  sp,  expireTime, timeUnit, redisTemplate, WAIT_TIME_OUT_SEC);
    }
    /**
     * 有缓存击穿的问题，只在特殊场景下使用
     * 比如部分数据可能存在直接修改数据库，导致缓存结果不一致
     * 同时不能接受缓存为空的代价，强制执行查询
     *
     * maxWaitTime 默认5秒
     * 为了防止过多线程等待的问题，设置最长等待时间
     * 执行时间不受限，请自行解决执行过慢的问题
     * 不希望通过工具去限制最长执行时间
     *
     * @throws TimeoutException
     */
    public static <T>List getListCacheNotNull(String key,Type type, Supplier<List<T>> sp, Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate) throws TimeoutException {
        return getListCacheNotNull(key, type,  sp,  expireTime, timeUnit, redisTemplate, WAIT_TIME_OUT_SEC);
    }
    /**
     * 有缓存击穿的问题，只在特殊场景下使用
     * 比如部分数据可能存在直接修改数据库，导致缓存结果不一致
     * 同时不能接受缓存为空的代价，强制执行查询
     *
     * 为了防止过多线程等待的问题，设置最长等待时间
     * 执行时间不受限，请自行解决执行过慢的问题
     * 不希望通过工具去限制最长执行时间
     * @throws TimeoutException
     */
    public static <T>List getListCacheNotNull(String key, Type type, Supplier<List<T>> sp, Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate, int maxWaitTime) throws TimeoutException {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setExpireTimeMs(TimeUnit.MILLISECONDS.convert(expireTime, timeUnit));
        cacheConfig.setMaxWaitTimeMs(TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS));
        cacheConfig.setIfNotNull(Boolean.TRUE);
        return getListCache(key, type, sp, redisTemplate, cacheConfig);
    }

    public static <T>T getCache(String key,Type type, Supplier<T> sp , Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate) throws TimeoutException {
        return getCache(key, type,  sp,  expireTime, timeUnit, redisTemplate, WAIT_TIME_OUT_SEC);
    }
    public static <T>T getCache(String key, Type type, Supplier<T> sp , Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate, int maxWaitTime) throws TimeoutException {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setExpireTimeMs(TimeUnit.MILLISECONDS.convert(expireTime, timeUnit));
        cacheConfig.setMaxWaitTimeMs(TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS));
        return getCache(key,type,sp, redisTemplate,cacheConfig);

    }

    public static <T>T getCache(String key, Type type, Supplier<T> sp , RedisTemplate redisTemplate, CacheConfig cacheConfig) throws TimeoutException {
        T t = null;
        String value = lockCacheGet(key, sp, redisTemplate,cacheConfig);
        if (value == null) {
            return null;
        }
        t = JSON.parseObject(value, type);

        return t;

    }

    /**
     * 有缓存击穿的问题，只在特殊场景下使用
     * 比如部分数据可能存在直接修改数据库，导致缓存结果不一致
     * 同时不能接受缓存为空的代价，强制执行查询
     *
     * @throws TimeoutException
     */
    public static <T>T getCacheNotNull(String key, Type type, Supplier<T> sp , Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate, int maxWaitTime) throws TimeoutException {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setExpireTimeMs(TimeUnit.MILLISECONDS.convert(expireTime, timeUnit));
        cacheConfig.setMaxWaitTimeMs(TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS));
        cacheConfig.setIfNotNull(true);

        return getCache(key,type,sp, redisTemplate,cacheConfig);
    }

    /**
     * 有缓存击穿的问题，只在特殊场景下使用
     * 比如部分数据可能存在直接修改数据库，导致缓存结果不一致
     * 同时不能接受缓存为空的代价，强制执行查询
     *
     * @throws TimeoutException
     */
    public static <T>T getCacheNotNull(String key,Type type, Supplier<T> sp , Long expireTime, TimeUnit timeUnit, RedisTemplate redisTemplate) throws TimeoutException {
        return getCacheNotNull(key, type,  sp,  expireTime, timeUnit, redisTemplate, WAIT_TIME_OUT_SEC);
    }

    /**
     * 在CPU和反应时间中尽量均衡
     * @param key
     * @param sp
     * @param redisTemplate
     * @param cacheConfig 详见 CacheConfig
     * @param <T>
     * @return
     * @throws TimeoutException
     */
    private static <T>String lockCacheGet(String key, Supplier<T> sp , RedisTemplate redisTemplate, CacheConfig cacheConfig) throws TimeoutException {
        String value = getFromRedis(key,  redisTemplate);
        if (value != null) {
            return dealValue(value, cacheConfig.getIfNotNull(), sp, key, cacheConfig.getExpireTimeMs(),cacheConfig.getNullExpireTimeMs(), redisTemplate);
        }
        long beginWait = System.currentTimeMillis();
        for ( ; ; ) {
            if(cacheConfig.getMaxWaitTimeMs() >0 && (System.currentTimeMillis()-beginWait)> cacheConfig.getMaxWaitTimeMs()){
                throw new TimeoutException("等待超时！");
            }
            if (lockByKey(key, cacheConfig.getLockTimeMs(),  redisTemplate)) {
                try {
                    value = getFromRedis(key, redisTemplate);
                    if(value != null){
                        return dealValue(value, cacheConfig.getIfNotNull(), sp, key, cacheConfig.getExpireTimeMs(),cacheConfig.getNullExpireTimeMs(), redisTemplate);
                    }
                }finally {
                    unlockByKey(key, redisTemplate);
                }
            }else if(cacheConfig.getMaxWaitTimeMs()<=0){
                throw new LockedFailedException("获取锁失败");
            }
            value = getFromRedis(key, redisTemplate);
            if (value != null) {
                return dealValue(value, cacheConfig.getIfNotNull(), sp, key, cacheConfig.getExpireTimeMs(),cacheConfig.getNullExpireTimeMs(), redisTemplate);
            }
            try {
                Thread.sleep(cacheConfig.getWaitSleepTimeMs());
            } catch (InterruptedException e) {
                LOGGER.error("Thread.sleep."+cacheConfig.getWaitSleepTimeMs(),e);
                //just ignore
            }
        }
    }

    private static <T> String dealValue(String value, boolean notNull, Supplier<T> sp, String key, Long expireTimeMs,Long nullExpireTimeMs, RedisTemplate redisTemplate) {
        value = unpackingValue(value);
        if(notNull && value == null){
            return doQueryCache(sp, key, expireTimeMs, nullExpireTimeMs, redisTemplate);
        }
        return value;
    }

    private static String unpackingValue(String value) {
        if (EMPTY_STRING.equals(value)) {
            return null;
        }
        return value;

    }

    private static <T> String doQueryCache(Supplier<T> sp,String key,  Long expireTimeMs,Long nullExpireTimeMs, RedisTemplate redisTemplate) {
        T t = sp.get();
        try {
            if (t == null || (t instanceof Collection && ((Collection) t).isEmpty())) {
                setRedis(key, null, nullExpireTimeMs == null ? expireTimeMs : nullExpireTimeMs, TimeUnit.MILLISECONDS,redisTemplate);
                return null;
            }
            String res = JSON.toJSONString(t);
            setRedis(key, res, expireTimeMs, TimeUnit.MILLISECONDS,redisTemplate);
        }catch (RedisInvalidException e){
            LOGGER.error("invalid redis error", e);
            throw e;
        }
        if (t == null || (t instanceof Collection && ((Collection) t).isEmpty())) {
            return null;
        }
        String res = JSON.toJSONString(t);
        return res;

    }


    private static Boolean lockByKey(String key,  Long lockTimeMs, RedisTemplate redisTemplate) throws RedisInvalidException {
        key = getLockKey(key);
        return transLock(key, lockTimeMs, TimeUnit.MILLISECONDS, redisTemplate);
    }
    private static Boolean unlockByKey(String key, RedisTemplate redisTemplate) throws RedisInvalidException {
        key = getLockKey(key);
        return delete(redisTemplate, key);
    }
    private static  String getLockKey(String key){
        return "@$%#lockByKeySec##"+key;
    }
    private static Boolean transLock(String key,  Long expireTime,TimeUnit timeUnit, RedisTemplate redisTemplate) throws RedisInvalidException {
        try {
            return setNx(redisTemplate, key, expireTime, timeUnit);
        }catch (Exception e){
            throw new RedisInvalidException("redis setIfAbsent 异常",e);
        }
    }
    private static void setRedis(String key, String value, long timeout, TimeUnit unit, RedisTemplate redisTemplate) throws RedisInvalidException {
        set(redisTemplate, key, value, timeout, unit);
    }
    private static String getFromRedis(String key, RedisTemplate redisTemplate) throws RedisInvalidException {
        try {
            return get(redisTemplate, key);
        }catch (Exception e){
            throw new RedisInvalidException("redis get 异常",e);
        }
    }

    public static boolean setNx(RedisTemplate redisTemplate, String key, Long expireTime, TimeUnit timeUnit) throws RedisInvalidException {
        return setNx(redisTemplate,key,expireTime,timeUnit,System.currentTimeMillis());
    }
    public static boolean setNx(RedisTemplate redisTemplate, String key, Long expireTime, TimeUnit timeUnit, Object value) throws RedisInvalidException {
        try {
            Object ob=  redisTemplate.execute((RedisCallback<Boolean>) connection -> {
                Object nativeConnection = connection.getNativeConnection();
                String res = null;
                // 集群模式（JedisCluster） 单机模式（Jedis）均可使用

                // 单机模式
                if (nativeConnection instanceof JedisCommands) {
                    res = ((JedisCommands) nativeConnection).set(key, JSON.toJSONString(value),"NX","EX",timeUnit.toSeconds(expireTime));
                }else{
                    throw new RuntimeException("invalid redis");
                }
                return Objects.equals("OK", res);
            });
            return (Boolean) ob;
        }catch (Exception e){
            throw new RedisInvalidException("redis setIfAbsent 异常",e);
        }
    }
    public static void  set(RedisTemplate redisTemplate , String key, String value, long timeout, TimeUnit unit)throws RedisInvalidException{
        try {
            if(value == null ){
                value = EMPTY_STRING;
            }
            redisTemplate.opsForValue().set(key, value, timeout, unit);
        }catch (Exception e){
            throw new RedisInvalidException("redis set 异常",e);
        }

    }
    public static String get(RedisTemplate redisTemplate, String key) throws RedisInvalidException {
        try {
            Object o= redisTemplate.opsForValue().get(key);
            return o!= null ? o.toString():null;
        }catch (Exception e){
            throw new RedisInvalidException("redis get 异常",e);
        }
    }
    public static <T>T get(RedisTemplate redisTemplate, String key, Type type) throws RedisInvalidException {
        try {
            String get= get(redisTemplate, key);
            if(StringUtils.isBlank(get)){
                return null;
            }
            T t  = JSON.parseObject(get, type);
            return t;
        }catch (Exception e){
            throw new RedisInvalidException("redis get 异常",e);
        }
    }
    public static <T>List getList(RedisTemplate redisTemplate, String key, Type type) throws RedisInvalidException {
        try {
            String value= get(redisTemplate, key);
            if (value == null) {
                return Collections.EMPTY_LIST;
            }
            List<T> t = JSON.parseObject(value, new TypeReference<List<T>>(type) {});

            return t;
        }catch (Exception e){
            throw new RedisInvalidException("redis get 异常",e);
        }
    }


    public static <T>List getListCache(RedisTemplate redisTemplate, String key, Type type) throws RedisInvalidException {
        Object value = get(redisTemplate, key);
        if (value == null || EMPTY_STRING.equals(value) ) {
            return Collections.EMPTY_LIST;
        }
        List<T>  t = JSON.parseObject(value.toString(), new TypeReference<List<T>>(type) {});
        return t;
    }
    public static <T>T getObjectCache(RedisTemplate redisTemplate, String key, Type type) throws RedisInvalidException {
        Object value = get(redisTemplate, key);
        if (value == null || EMPTY_STRING.equals(value) ) {
            return null;
        }
        T t = JSON.parseObject(value.toString(), type);
        return t;
    }
    public static Boolean delete(RedisTemplate redisTemplate, String key){
        return redisTemplate.delete(key);
    }
    public static Long increBy(RedisTemplate redisTemplate, String key){
        return  redisTemplate.opsForValue().increment(key, 1);
    }
    public static Long increBy(RedisTemplate redisTemplate, String key, Long time, TimeUnit timeUnit){
        try {
            Object ob =  redisTemplate.execute((RedisCallback<Long>) connection -> {
                Object nativeConnection = connection.getNativeConnection();
                Long times = null;
                String sec = String.valueOf(timeUnit.toSeconds(time));
                // 集群模式（JedisCluster） 单机模式（Jedis）均可使用
                if (nativeConnection instanceof JedisCluster) {
                    times = (Long) (((JedisCluster) nativeConnection).eval(LUA_INCR.toString(),Collections.singletonList(key),Collections.singletonList(sec)));
                }else if(nativeConnection instanceof Jedis){
                    times = (Long) ((Jedis) nativeConnection).eval(LUA_INCR.toString(),Collections.singletonList(key),Collections.singletonList(sec));
                }else {
                    throw new RuntimeException("not support method for :"+nativeConnection.getClass());
                }
                return times;

            });
            return (Long)ob;
        }catch (Exception e){
            throw new RedisInvalidException("redis setIfAbsent 异常",e);
        }
    }
    public static boolean  expire(RedisTemplate redisTemplate, String key, Long time, TimeUnit timeUnit){
        return redisTemplate.expire(key, time, timeUnit);
    }


    public static class RedisInvalidException extends RuntimeException{
        public RedisInvalidException() {
            super();
        }

        public RedisInvalidException(String message) {
            super(message);
        }

        public RedisInvalidException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    public static class LockedFailedException extends RuntimeException{
        public LockedFailedException() {
            super();
        }

        public LockedFailedException(String message) {
            super(message);
        }

        public LockedFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    public static class CacheConfig{
        //key 失效时间
        private long expireTimeMs;
        //如果内容为空失效时间 默认为等同expireTimeMs
        private long nullExpireTimeMs;
        //最长等待时间 如果小于等于0，无法查询到结果且无法获取锁则直接抛出 默认5000毫秒
        private long maxWaitTimeMs;
        //等待时内线程休眠时间 默认50ms
        private long waitSleepTimeMs = 50;
        //分布式锁等待时长 默认为600s
        private long lockTimeMs = 600*1000;
        //是否允许缓存为空，不允许则执行后续操作 默认允许为空
        private boolean ifNotNull = Boolean.FALSE;

        public long getExpireTimeMs() {
            return expireTimeMs;
        }

        public void setExpireTimeMs(long expireTimeMs) {
            this.expireTimeMs = expireTimeMs;
        }

        public long getNullExpireTimeMs() {
            return nullExpireTimeMs;
        }

        public void setNullExpireTimeMs(long nullExpireTimeMs) {
            this.nullExpireTimeMs = nullExpireTimeMs;
        }

        public long getMaxWaitTimeMs() {
            return maxWaitTimeMs;
        }

        public void setMaxWaitTimeMs(long maxWaitTimeMs) {
            this.maxWaitTimeMs = maxWaitTimeMs;
        }

        public long getWaitSleepTimeMs() {
            return waitSleepTimeMs;
        }

        public void setWaitSleepTimeMs(long waitSleepTimeMs) {
            this.waitSleepTimeMs = waitSleepTimeMs;
        }

        public long getLockTimeMs() {
            return lockTimeMs;
        }

        public void setLockTimeMs(long lockTimeMs) {
            this.lockTimeMs = lockTimeMs;
        }

        public Boolean getIfNotNull() {
            return ifNotNull;
        }

        public void setIfNotNull(Boolean ifNotNull) {
            this.ifNotNull = ifNotNull;
        }
    }


}
