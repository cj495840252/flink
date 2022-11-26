package org.flink.demos.window;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
/**
 * 滚动窗口：10一次
 * 若其中Event的msg = click时也触发一次计算
 */
public class MyTrigger extends Trigger<EventBean, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private MyTrigger() {
    }

    public TriggerResult onElement(EventBean element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        /*来一条数据时，检查watermark是否已经超过窗口结束点，超过则触发*/
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            // 注册定时器的的触发时间
            ctx.registerEventTimeTimer(window.maxTimestamp());
            if ("click".equals(element.getMsg()))return TriggerResult.FIRE;
            return TriggerResult.CONTINUE;
        }
    }

    public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) {
        // 事件定时器的触发时间（窗口结束时间）到达了，检查是否满足触发条件
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        // 处理事件定时器的触发时间（窗口结束时间）到达了，检查是否满足触发条件
        return TriggerResult.CONTINUE;
    }

    public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public boolean canMerge() {
        return true;
    }

    public void onMerge(TimeWindow window, Trigger.OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }

    }

    public String toString() {
        return "EventTimeTrigger()";
    }

    public static MyTrigger create() {
        return new MyTrigger();
    }

}
