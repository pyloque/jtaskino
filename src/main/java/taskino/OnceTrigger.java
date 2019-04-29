package taskino;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class OnceTrigger implements Trigger {

    private Date startTime;

    public OnceTrigger() {}

    public OnceTrigger(Date startTime) {
        this.startTime = startTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    @Override
    public TriggerType type() {
        return TriggerType.ONCE;
    }

    @Override
    public void parse(String s) {
        var formatter = TimeFormat.ISOFormatter.take();
        this.startTime = formatter.parseQuitely(s);
    }

    @Override
    public String serialize() {
        var formatter = TimeFormat.ISOFormatter.take();
        return formatter.format(this.startTime);
    }
    
    @Override
    public ScheduledFuture<?> schedule(ScheduledExecutorService scheduler,
                    ExecutorService executor, Predicate<Task> taskTaker, Task task) {
        long now = System.currentTimeMillis();
        var delay = this.getStartTime().getTime() - now;
        if (delay > 0) {
            return scheduler.schedule(() -> {
                if (taskTaker.test(task)) {
                    executor.submit(task);
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
        return null;
    }

}
