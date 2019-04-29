package taskino;

import java.util.Map;

public interface ITaskStore {

    /**
     * 存储器中任务列表版本号
     * 
     * @return
     */
    public long getRemoteVersion();

    /**
     * 获取所有的任务触发器
     * 
     * @return
     */
    public Map<String, String> getAllTriggers();

    /**
     * 保存所有的任务触发器
     * 
     * @param version
     * @param triggers
     */
    public void saveAllTriggers(long version, Map<String, String> triggers);

    /**
     * 抢占任务（是否可以抢到任务）
     * 
     * @param name
     * @return
     */
    public boolean grabTask(String name);

}
