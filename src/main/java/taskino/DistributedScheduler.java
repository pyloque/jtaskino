package taskino;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每个进程的内存中都会保存所有的任务实例 到点就会运行相应的任务
 * 
 * 通过 Redis 的分布式锁来控制互斥（同一时间只允许单个进程运行任务）
 * 
 * 调度器是单一的线程
 * 
 * 任务运行器是一个线程池，默认大小为 cores * 2
 * 
 * 利用全局版本号来控制任务重加载（变更的任务）
 * 
 * @author qianwp
 *
 */
public class DistributedScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(DistributedScheduler.class);

    // 任务触发器存储器，持久化
    private ITaskStore store;

    // 任务列表版本号（任务重加载）
    private long version;

    // 所有的任务
    private Map<String, Task> allTasks = new HashMap<>();

    // 任务触发器
    private Map<String, Trigger> triggers = new HashMap<>();

    // 任务调度器（调度线程）
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // 任务运行器（运行线程）
    private ExecutorService executor;

    // 任务完成监听器
    private List<ISchedulerListener> listeners = new ArrayList<>();

    // 待重加载的触发器
    private Map<String, Trigger> reloadingTriggers = new HashMap<>();

    public DistributedScheduler(ITaskStore store) {
        this(store, Runtime.getRuntime().availableProcessors() * 2);
    }

    /**
     * @param store 任务存储器
     * @param threads 任务运行线程数（默认 cores*2）
     */
    public DistributedScheduler(ITaskStore store, int threads) {
        this.store = store;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    /**
     * 注册调度监听器
     * 
     * @param listener
     * @return
     */
    public DistributedScheduler listener(ISchedulerListener listener) {
        this.listeners.add(listener);
        return this;
    }

    /**
     * 注册任务
     * 
     * @param trigger
     * @param task
     * @return
     */
    public DistributedScheduler register(Trigger trigger, Task task) {
        if (this.triggers.containsKey(task.name())) {
            throw new IllegalArgumentException("task name duplicated!");
        }
        this.triggers.put(task.name(), trigger);
        this.allTasks.put(task.name(), task);
        task.callback(ctx -> {
            for (var listener : listeners) {
                try {
                    listener.onComplete(ctx);
                } catch (Exception ex) {
                    LOG.error("invoke task {} complete listener error", ctx.task().name(), ex);
                }
            }
        });
        return this;
    }

    /**
     * 手动触发任务运行
     * 
     * @param name
     */
    public void triggerTask(String name) {
        var task = this.allTasks.get(name);
        if (task != null) {
            task.run();
        }
    }

    /**
     * 任务变更时，必须递增版本号，才能触发其它进程的任务重新加载
     * 
     * @param version
     * @return
     */
    public DistributedScheduler version(int version) {
        if (version < 0) {
            throw new IllegalArgumentException("tasks version must be non-negative!");
        }
        this.version = version;
        return this;
    }

    /**
     * 启动调度器
     */
    public void start() {
        // 先保存触发器（如果任务变更，会触发其它进程重加载）
        this.saveTriggers();
        // 调度任务
        this.scheduleTasks();
        // 监控任务版本（重加载）
        this.scheduleReload();
        // 回调
        for (var listener : listeners) {
            try {
                listener.onStartup();
            } catch (Exception e) {
                LOG.error("invoke scheduler startup listener error", e);
            }
        }
    }

    private void saveTriggers() {
        var triggersRaw = new HashMap<String, String>();
        this.triggers.forEach((name, trigger) -> {
            triggersRaw.put(name, trigger.s());
        });
        this.store.saveAllTriggers(version, triggersRaw);
    }

    private void scheduleTasks() {
        this.triggers.forEach((name, trigger) -> {
            var task = allTasks.get(name);
            if (task == null) {
                return;
            }
            LOG.info("scheduling task {}", name);
            trigger.schedule(scheduler, executor, this::grabTaskSilently, task);
        });
    }

    private boolean grabTaskSilently(Task task) {
        if (task.isConcurrent()) {
            return true;
        }
        try {
            return store.grabTask(task.name());
        } catch (Exception e) {
            LOG.error("taking task {} error", task.name(), e);
            return false;
        }
    }

    private synchronized void rescheduleTasks() {
        this.reloadingTriggers.forEach((name, trigger) -> {
            var task = this.allTasks.get(name);
            if (trigger == null) {
                // deleting
                LOG.warn("unscheduling task {}", name);
                triggers.get(name).cancel();
                triggers.remove(name);
            } else {
                var oldTrigger = triggers.get(name);
                if (oldTrigger != null) {
                    // updating, cancel the old first
                    LOG.warn("unscheduling task {}", name);
                    oldTrigger.cancel();
                }
                triggers.put(name, trigger);
                // new
                LOG.warn("scheduling task {}", name);
                trigger.schedule(scheduler, executor, this::grabTaskSilently, task);
            }
        });
        this.reloadingTriggers.clear();
        // 回调
        for (var listener : listeners) {
            try {
                listener.onReload();
            } catch (Exception e) {
                LOG.error("invoke scheduler reload listener error", e);
            }
        }
    }

    private synchronized void cancelAllTasks() {
        this.triggers.forEach((name, trigger) -> {
            LOG.warn("cancelling task {}", name);
            trigger.cancel();
        });
        this.triggers.clear();
    }

    public void stop() {
        this.cancelAllTasks();
        this.scheduler.shutdown();
        try {
            this.scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        this.executor.shutdown();
        try {
            if (!this.executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.warn("work is not complete while stopping scheduler");
            }
        } catch (InterruptedException e) {
        }
        // 回调
        for (var listener : listeners) {
            try {
                listener.onStop();
            } catch (Exception e) {
                LOG.error("invoke scheduler stop listener error", e);
            }
        }
    }

    private void scheduleReload() {
        // 1s 对比一次
        this.scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (this.reloadIfChanged()) {
                    this.rescheduleTasks();
                }
            } catch (Exception e) {
                LOG.error("reloading tasks error", e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private boolean reloadIfChanged() {
        var remoteVersion = store.getRemoteVersion();
        if (remoteVersion > version) {
            this.version = remoteVersion;
            LOG.warn("version changed! reload triggers then reschedule changed tasks");
            this.reload();
            return true;
        }
        return false;
    }

    private void reload() {
        var raws = store.getAllTriggers();
        var reloadings = new HashMap<String, Trigger>();
        raws.forEach((name, raw) -> {
            // 内存里必须有这个任务（新增任务，老版本的进程里就没有）
            if (this.allTasks.containsKey(name)) {
                var trigger = Trigger.build(raw);
                var oldTrigger = this.triggers.get(name);
                if (oldTrigger == null || !oldTrigger.equals(trigger)) {
                    // new or changed
                    reloadings.put(name, trigger);
                }
            }
        });
        // deleted
        this.triggers.forEach((name, trigger) -> {
            if (!raws.containsKey(name)) {
                reloadings.put(name, null);
            }
        });
        this.reloadingTriggers = reloadings;
    }

    public static void main(String[] args) {
        var redis = new RedisStore();
        var store = new RedisTaskStore(redis, "sample");
        var scheduler = new DistributedScheduler(store, 5);
        scheduler.register(Trigger.once(new Date(0)), Task.of("once1", () -> {
            System.out.println("once1");
        }));
//        scheduler.register(Trigger.period(new Date(0), 5), Task.of("period2", () -> {
//            System.out.println("period2");
//        }));
        scheduler.register(Trigger.cronOfMinutes(2), Task.of("cron3", () -> {
            System.out.println("cron3");
        }));
        scheduler.register(Trigger.period(new Date(0), 10), Task.of("period4", () -> {
            System.out.println("period4");
        }));
        scheduler.version(4);
        scheduler.listener(ctx -> {
            System.out.println(ctx.task().name() + " is complete");
        });
        scheduler.start();
    }

}
