package taskino;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;
import it.sauronsoftware.cron4j.SchedulingPattern;

public interface Trigger {

    public TriggerType type();

    public void parse(String s);

    public String serialize();

    public default String s() {
        return String.format("%s@%s", type().name(), serialize());
    }

    public ScheduledFuture<?> schedule(ScheduledExecutorService scheduler, ExecutorService executor,
                    Predicate<Task> taskTaker, Task task);


    public static Trigger build(String s) {
        var parts = s.split("@", 2);
        var type = parts[0];
        Trigger trigger = null;
        switch (TriggerType.valueOf(type)) {
            case ONCE:
                trigger = new OnceTrigger();
                break;
            case PERIOD:
                trigger = new PeriodTrigger();
                break;
            case CRON:
                trigger = new CronTrigger();
                break;
        }
        trigger.parse(parts[1]);
        return trigger;
    }

    public static OnceTrigger once(Date startTime) {
        return new OnceTrigger(startTime);
    }

    public static OnceTrigger onceOfDelay(int delay) {
        var now = new Date();
        var cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.SECOND, delay);
        return once(cal.getTime());
    }

    public static PeriodTrigger period(Date startTime, Date endTime, int period) {
        return new PeriodTrigger(startTime, endTime, period);
    }

    public static PeriodTrigger periodOfDelay(int delay, int period) {
        var now = new Date();
        var cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.SECOND, delay);
        return new PeriodTrigger(cal.getTime(), new Date(Long.MAX_VALUE), period);
    }

    public static CronTrigger cron(String expr) {
        if (!SchedulingPattern.validate(expr)) {
            throw new IllegalArgumentException("cron expression illegal");
        }
        return new CronTrigger(expr);
    }

    public static CronTrigger cronOfMinutes(int minutes) {
        return cron(String.format("*/%d * * * *", minutes));
    }

    public static CronTrigger cronOfHours(int hours, int minuteOffset) {
        return cron(String.format("%d */%d * * *", minuteOffset, hours));
    }

    public static CronTrigger cronOfDays(int days, int hourOffset, int minuteOffset) {
        return cron(String.format("%d %d */%d * *", minuteOffset, hourOffset, days));
    }
}
