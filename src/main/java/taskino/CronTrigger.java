package taskino;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import it.sauronsoftware.cron4j.SchedulingPattern;

public class CronTrigger implements Trigger {

    private String cronExpr;
    private ScheduledFuture<?> future;

    public CronTrigger() {}

    public CronTrigger(String cronExpr) {
        this.cronExpr = cronExpr;
    }

    public String getCronExpr() {
        return cronExpr;
    }

    @Override
    public TriggerType type() {
        return TriggerType.CRON;
    }

    @Override
    public void parse(String s) {
        this.cronExpr = s;
    }

    @Override
    public String serialize() {
        return this.cronExpr;
    }

    @Override
    public boolean schedule(ScheduledExecutorService scheduler, ExecutorService executor,
                    Predicate<Task> taskTaker, Task task) {
        var pattern = new SchedulingPattern(this.getCronExpr());
        // 将毫秒数清零，确保多进程同一时间争抢
        Calendar cal = Calendar.getInstance();
        var now = new Date();
        cal.setTime(now);
        cal.set(Calendar.MILLISECOND, 0);
        // 如果正好卡在分点上（second=0）那就立即执行
        if (cal.get(Calendar.SECOND) != 0) {
            cal.set(Calendar.SECOND, 0);
            cal.add(Calendar.MINUTE, 1);
        }
        long delay = cal.getTimeInMillis() - now.getTime();
        this.future = scheduler.scheduleAtFixedRate(() -> {
            if (pattern.match(System.currentTimeMillis())) {
                if (taskTaker.test(task)) {
                    executor.submit(task);
                }
            }
        }, delay, 60 * 1000, TimeUnit.MILLISECONDS);
        return this.future != null;
    }

    @Override
    public void cancel() {
        if (this.future != null) {
            this.future.cancel(false);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CronTrigger)) {
            return false;
        }
        return this.cronExpr.equals(((CronTrigger) other).cronExpr);
    }

}
