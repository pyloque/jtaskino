折腾了一周的 Java Quartz 集群任务调度，很遗憾没能搞定，网上的相关文章也少得可怜，在多节点（多进程）环境下 Quartz 似乎无法动态增减任务，恼火。无奈之下自己撸了一个简单的任务调度器，结果只花了不到 2天时间，而且感觉非常简单好用，代码量也不多，扩展性很好。

![](https://user-gold-cdn.xitu.io/2019/4/29/16a682144a242fdf?w=776&h=265&f=png&s=22307)

实现一个分布式的任务调度器有几个关键的考虑点

1. 单次任务和循环任务好做，难的是 cron 表达式的解析和时间计算怎么做？
2. 多进程同一时间如何保证一个任务的互斥性？
3. 如何动态变更增加和减少任务？

## 代码实例

在深入讲解实现方法之前，我们先来看看这个调度器是如何使用的

```java
class Demo {
    public static void main(String[] args) {
        var redis = new RedisStore();
        // sample 为任务分组名称
        var store = new RedisTaskStore(redis, "sample");
        // 5s 为任务锁寿命
        var scheduler = new DistributedScheduler(store, 5);
        // 注册一个单次任务
        scheduler.register(Trigger.onceOfDelay(5), Task.of("once1", () -> {
            System.out.println("once1");
        }));
        // 注册一个循环任务
        scheduler.register(Trigger.periodOfDelay(5, 5), Task.of("period2", () -> {
            System.out.println("period2");
        }));
        // 注册一个 CRON 任务
        scheduler.register(Trigger.cronOfMinutes(1), Task.of("cron3", () -> {
            System.out.println("cron3");
        }));
        // 设置全局版本号
        scheduler.version(1);
        // 注册监听器
        scheduler.listener(ctx -> {
            System.out.println(ctx.task().name() + " is complete");
        });
        // 启动调度器
        scheduler.start();
    }
}
```

当代码升级任务需要增加减少时（或者变更调度时间），只需要递增全局版本号，现有的进程中的任务会自动被重新调度，那些没有被注册的任务（任务减少）会自动清除。新增的任务（新任务）在老代码的进程里是不会被调度的（没有新任务的代码无法调度），被清除的任务（老任务）在老代码的进程里会被取消调度。

比如我们要取消 period2 任务，增加 period4 任务

```java
class Demo {
    public static void main(String[] args) {
        var redis = new RedisStore();
        // sample 为任务分组名称
        var store = new RedisTaskStore(redis, "sample");
        // 5s 为任务锁寿命
        var scheduler = new DistributedScheduler(store, 5);
        // 注册一个单次任务
        scheduler.register(Trigger.onceOfDelay(5), Task.of("once1", () -> {
            System.out.println("once1");
        }));
        // 注册一个 CRON 任务
        scheduler.register(Trigger.cronOfMinutes(1), Task.of("cron3", () -> {
            System.out.println("cron3");
        }));
        // 注册一个循环任务
        scheduler.register(Trigger.periodOfDelay(5, 10), Task.of("period4", () -> {
            System.out.println("period4");
        }));
        // 递增全局版本号
        scheduler.version(2);
        // 注册监听器
        scheduler.listener(ctx -> {
            System.out.println(ctx.task().name() + " is complete");
        });
        // 启动调度器
        scheduler.start();
    }
}
```

## 原理

https://juejin.im/post/5cc6ade4f265da038d0b4fd6
