package taskino;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Task.class);

    /**
     * 是否需要考虑多进程互斥（true表示不互斥，多进程能同时跑）
     */
    private boolean concurrent;

    private String name;

    private Runnable runner;

    private transient Consumer<TaskContext> callback = ctx -> {
    };

    public Task(String name, boolean concurrent, Runnable runner) {
        this.name = name.toLowerCase();
        this.concurrent = concurrent;
        this.runner = runner;
    }

    public static Task of(String name, Runnable runner) {
        return new Task(name, false, runner);
    }

    public static Task concurrent(String name, Runnable runner) {
        return new Task(name, true, runner);
    }

    public boolean isConcurrent() {
        return concurrent;
    }

    public String name() {
        return name;
    }

    protected void callback(Consumer<TaskContext> callback) {
        this.callback = callback;
    }

    public void run() {
        boolean ok = false;
        Throwable ex = null;
        long startTs = System.currentTimeMillis();
        try {
            runner.run();
            ok = true;
        } catch (Exception e) {
            LOG.error("running task {} error", name, e);
            ex = e;
        }
        long cost = System.currentTimeMillis() - startTs;
        var ctx = new TaskContext(this, cost, ok, ex);
        callback.accept(ctx);
    }

}
