package com.github.zhufg.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @auth zhufg
 *  线程池实现，请尽量不要使用thread.sleep
 */
public class ThreadPoolUtil {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolUtil.class);
    private static final Map<String, ExecutorService> executorMap = new ConcurrentHashMap<String, ExecutorService>();
    private static final int availableProcessors = Runtime.getRuntime().availableProcessors();

    private static ExecutorService getExecutor(String name) {
        String executorName = new StringBuilder("EXECUTOR_").append(name).toString();
        ExecutorService executorOne = executorMap.get(executorName);
        if (executorOne == null) {
            synchronized (executorName) {
                executorOne = executorMap.get(executorName);
                if (executorOne == null) {
                    executorOne = newThreadPool();
                    executorMap.put(executorName, executorOne);
                }
            }

        }
        return executorOne;
    }

    public static void execute(String name, Runnable command) {
        ExecutorService executorService = getExecutor(name);
        executorService.execute(command);
    }
    public static <T> Future<T> submit(String name, Callable<T> command) {
        ExecutorService executorService = getExecutor(name);
        return executorService.submit(command);
    }
    public static void executeByCommon(Runnable command) {
        execute(null, command);
    }

    public static void execute(String name, Runnable command, int timeoutSec)throws TimeoutException  {
        ExecutorService executorService = getExecutor(name);
        Future<?> future = executorService.submit(command);
        try {
            future.get(timeoutSec, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error(name + "InterruptedException error", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.error(name + " ExecutionException error", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> List<List<T>> partitionList(List<T> list, int parSize) {
        if (parSize <= 0) {
            throw new RuntimeException("error partition by parSize error");
        }
        List<List<T>> lists = new ArrayList<>(parSize);
        if (list == null || list.size() == 0) {
            return lists;
        }
        if (parSize == 1) {
            lists.add(list);
            return lists;
        }
        int listSize = list.size();
        int pageSize = listSize / parSize;
        if (pageSize <= 0) {
            pageSize = 1;
        }
        int addOnes = listSize >= parSize ? listSize % parSize : 0;
        for (int beginIndex = 0; beginIndex <= listSize - 1;) {
            int endIndex = beginIndex + pageSize;
            if (addOnes-- > 0) {
                endIndex++;
            }
            if (endIndex > listSize) {
                endIndex = listSize;
            }
            List<T> parList = list.subList(beginIndex, endIndex);
            lists.add(parList);
            beginIndex = endIndex;
        }
        return lists;

    }
    public static <T> List<List<T>> perSize(List<T> list, int perSize) {
        if (perSize <= 0) {
            throw new RuntimeException("error partition by perSize error");
        }
        List<List<T>> lists = new ArrayList<List<T>>(perSize);
        if (list == null || list.size() == 0) {
            return lists;
        }
        int size = list.size();
        for(int fromIndex = 0 ; fromIndex< list.size(); fromIndex+=perSize){
            int endIndex = fromIndex+perSize > size?size:fromIndex+perSize;
            lists.add(list.subList(fromIndex, endIndex));
        }
        return lists;

    }

    public static <T> List<List<T>> partitionListByProcess(List<T> list) {
        return partitionList(list, getReasonableProcess());
    }

    public static int getReasonableProcess() {
        return availableProcessors;
    }

    private static ExecutorService newThreadPool(){
        return Executors.newWorkStealingPool(getReasonableProcess());
    }
    public enum PoolExceptionPolicy{
        IGNORE,
        SHUTDOWN,
        RETURNRULSTNOW;
    }
    public static CountDownLatchHerlper getResultHelper(String taskName, int timeoutSec, PoolExceptionPolicy poolExceptionPolicy){
        return new CountDownLatchHerlper(taskName, timeoutSec, poolExceptionPolicy);
    }
    public static ResultVoidHerlper getVoidHelper(String taskName, int timeoutSec,PoolExceptionPolicy poolExceptionPolicy){
        return new ResultVoidHerlper(taskName, timeoutSec, poolExceptionPolicy);
    }
    public static class CountDownLatchHerlper<T>{
        private String taskName;
        private int timeoutSec;
        private PoolExceptionPolicy poolExceptionPolicy;
        private AtomicInteger taskNums = new AtomicInteger(0);
        private long beginTime;
        private long endTime;
        private long lastMillis;
        private volatile int taskStatus;//0未执行1执行中2执行完毕
        private List<Future<T>>  futures = new ArrayList<>();
        List<T> results = new ArrayList<>();
        private List<Exception> exs = new ArrayList<>();
        private CountDownLatchHerlper(String taskName, int timeoutSec,PoolExceptionPolicy poolExceptionPolicy){
            this.taskName  = taskName;
            this.timeoutSec = timeoutSec;
            this.poolExceptionPolicy = poolExceptionPolicy;
        }
        public CountDownLatchHerlper addTask(Callable<T> call){
            if(taskNums.addAndGet(1) ==1){
                beginTime = System.currentTimeMillis();
                lastMillis = beginTime+timeoutSec*1000;
            }
            if(this.taskStatus == 2){
                throw new RuntimeException("任务已执行完毕，无法添加任务");
            }
            this.taskStatus = 1;
            futures.add(submit(taskName, call));
            return this;
        }

        public List<T> getTaskResults() throws TimeoutException{
            for(Future<T> t :futures ){
                try {
                    if(lastMillis < System.currentTimeMillis()){
                        throw new TimeoutException("执行任务超时！");
                    }
                    if(t.isDone()){
                        results.add(t.get());
                    }else{
                        results.add(t.get(lastMillis-System.currentTimeMillis(), TimeUnit.MILLISECONDS));
                    }
                }  catch (TimeoutException e) {
                    this.taskStatus = 3;
                    if(PoolExceptionPolicy.SHUTDOWN.equals(poolExceptionPolicy)){
                        throw e;
                    }
                    logger.error("执行异常，根据执行策略，直接返回现有结果", e);
                    results = getTaskRes();
                    this.exs.add(e);
                    throw e;
                }catch (Exception e) {
                    this.taskStatus = 3;
                    if(PoolExceptionPolicy.SHUTDOWN.equals(poolExceptionPolicy)){
                        this.endTime = System.currentTimeMillis();
                        throw new RuntimeException(e);
                    }
                    if(PoolExceptionPolicy.RETURNRULSTNOW.equals(poolExceptionPolicy)){
                        logger.error("执行异常，根据执行策略，直接返回现有结果", e);
                        results = getTaskRes();
                        this.exs.add(e);
                        this.endTime = System.currentTimeMillis();
                        return results;
                    }
                    if(PoolExceptionPolicy.IGNORE.equals(poolExceptionPolicy)){
                        logger.error("执行异常，根据执行策略，继续执行", e);
                        this.exs.add(e);
                        continue;
                    }
                }
            }
            if(this.taskStatus  == 1){
                this.taskStatus =2;
            }
            this.endTime = System.currentTimeMillis();
            logger.debug("执行任务结束执行任务完成时间为"+(this.endTime-this.beginTime)+"&总执行任务数："+taskNums);
            return results;
        }
        private List<T> getTaskRes(){
            List<T> results = new ArrayList<>();
            for(Future<T> t :futures ){
                if(t.isDone()){
                    try {
                        results.add(t.get());
                    } catch (InterruptedException e) {
                        // just ignore
                    } catch (ExecutionException e) {
                        // just ignore
                    }
                }else {
                    t.cancel(true);
                }
            }
            return results;
        }
        public <V> V getResultWithDealer(Function<List<T>,V> fc)throws TimeoutException{
            return fc.apply(getTaskResults());
        }
        public long getTaskExecuteTime(){
            if(this.taskStatus != 2 && this.taskStatus != 3){
                throw new RuntimeException("执行异常，任务未完成无法获取执行时间");
            }
            return this.endTime-this.beginTime;
        }

        public int getTaskStatus() {
            return taskStatus;
        }

        public List<T> getResults() {
            return results;
        }

        public List<Exception> getExs() {
            return exs;
        }
    }
    public static class ResultVoidHerlper<T>{
        private String taskName;
        private int timeoutSec;
        private PoolExceptionPolicy poolExceptionPolicy;
        private AtomicInteger taskNums = new AtomicInteger(0);
        private long beginTime;
        private long endTime;
        private long lastMillis;
        private volatile int taskStatus;//0未执行1执行中2执行完毕3执行中有异常
        private List<Future<T>>  futures = new ArrayList<>();
        private List<Exception> exs = new ArrayList<>();
        private ResultVoidHerlper(String taskName, int timeoutSec,PoolExceptionPolicy poolExceptionPolicy){
            this.taskName  = taskName;
            this.timeoutSec = timeoutSec;
            this.poolExceptionPolicy = poolExceptionPolicy;
        }
        public ResultVoidHerlper addTask(Runnable run){
            if(taskNums.addAndGet(1) ==1){
                beginTime = System.currentTimeMillis();
                lastMillis = beginTime+timeoutSec*1000;
            }
            if(this.taskStatus >=2){
                throw new RuntimeException("任务已执行完毕，无法添加任务");
            }
            this.taskStatus = 1;
            Future<T> t = submit(taskName, new Callable<T>(){
                @Override
                public T call() throws Exception {
                    run.run();
                    return (T)Integer.valueOf(1);
                }
            });
            futures.add(t);
            return this;
        }
        public void doneTask() throws TimeoutException {
            for (Future<T> t : futures) {
                try {
                    if (lastMillis < System.currentTimeMillis()) {
                        throw new TimeoutException("执行任务超时！");
                    }
                    if(t.isDone()){
                        continue;
                    }
                    t.get(lastMillis-System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    this.taskStatus =3;
                    this.endTime = System.currentTimeMillis();
                    this.exs.add(e);
                    doneInProgress();
                    throw e;
                } catch (Exception e) {
                    this.taskStatus =3;
                    if (PoolExceptionPolicy.SHUTDOWN.equals(poolExceptionPolicy)) {
                        this.endTime = System.currentTimeMillis();
                        doneInProgress();
                        throw new RuntimeException(e);
                    }
                    if (PoolExceptionPolicy.RETURNRULSTNOW.equals(poolExceptionPolicy)) {
                        logger.error("执行异常，根据执行策略，直接返回现有结果", e);
                        this.exs.add(e);
                        this.endTime = System.currentTimeMillis();
                        doneInProgress();
                        return;
                    }
                    if(PoolExceptionPolicy.IGNORE.equals(poolExceptionPolicy)){
                        logger.error("执行异常，根据执行策略，继续执行", e);
                        this.exs.add(e);
                        continue;
                    }
                }
                if(this.taskStatus  == 1){
                    this.taskStatus =2;
                }
                this.endTime = System.currentTimeMillis();
                logger.info("执行任务结束执行任务完成时间为" + (this.endTime - this.beginTime) + "&总执行任务数：" + taskNums);
            }
        }

        private void doneInProgress() {
            for(Future<T> t :futures ){
                if(t.isDone()){
                    continue;
                }else {
                    t.cancel(true);
                }
            }
        }

        public long getTaskExecuteTime(){
            if(this.taskStatus != 2 && this.taskStatus != 3){
                throw new RuntimeException("执行异常，任务未完成无法获取执行时间");
            }
            return this.endTime-this.beginTime;
        }

        public int getTaskStatus() {
            return taskStatus;
        }



        public List<Exception> getExs() {
            return exs;
        }

    }
}
