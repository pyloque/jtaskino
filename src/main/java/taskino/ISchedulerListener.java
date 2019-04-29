package taskino;

public interface ISchedulerListener {

    public void onComplete(TaskContext ctx);

    public default void onStartup() {}

    public default void onReload() {}

    public default void onStop() {}

}
