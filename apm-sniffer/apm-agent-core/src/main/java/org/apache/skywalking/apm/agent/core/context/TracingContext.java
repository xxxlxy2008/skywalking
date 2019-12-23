/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.context;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.trace.*;
import org.apache.skywalking.apm.agent.core.dictionary.*;
import org.apache.skywalking.apm.agent.core.logging.api.*;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * The <code>TracingContext</code> represents a core tracing logic controller. It build the final {@link
 * TracingContext}, by the stack mechanism, which is similar with the codes work.
 *
 * In opentracing concept, it means, all spans in a segment tracing context(thread) are CHILD_OF relationship, but no
 * FOLLOW_OF.
 *
 * In skywalking core concept, FOLLOW_OF is an abstract concept when cross-process MQ or cross-thread async/batch tasks
 * happen, we used {@link TraceSegmentRef} for these scenarios. Check {@link TraceSegmentRef} which is from {@link
 * ContextCarrier} or {@link ContextSnapshot}.
 *
 * @author wusheng
 * @author zhang xin
 */
public class TracingContext implements AbstractTracerContext {
    private static final ILog logger = LogManager.getLogger(TracingContext.class);
    private long lastWarningTimestamp = 0;

    /**
     * @see {@link SamplingService}
     */
    private SamplingService samplingService;

    /**
     * The final {@link TraceSegment}, which includes all finished spans.
     */
    private TraceSegment segment;

    /**
     * Active spans stored in a Stack, usually called 'ActiveSpanStack'. This {@link LinkedList} is the in-memory
     * storage-structure. <p> I use {@link LinkedList#removeLast()}, {@link LinkedList#addLast(Object)} and {@link
     * LinkedList#last} instead of {@link #pop()}, {@link #push(AbstractSpan)}, {@link #peek()}
     */
    private LinkedList<AbstractSpan> activeSpanStack = new LinkedList<AbstractSpan>();

    /**
     * A counter for the next span.
     */
    private int spanIdGenerator;

    /**
     * The counter indicates
     */
    private volatile AtomicInteger asyncSpanCounter;
    private volatile boolean isRunningInAsyncMode;
    private volatile ReentrantLock asyncFinishLock;

    /**
     * Initialize all fields with default value.
     */
    TracingContext() {
        this.segment = new TraceSegment();
        this.spanIdGenerator = 0;
        samplingService = ServiceManager.INSTANCE.findService(SamplingService.class);
        isRunningInAsyncMode = false;
    }

    /**
     * Inject the context into the given carrier, only when the active span is an exit one.
     *
     * @param carrier to carry the context for crossing process.
     * @throws IllegalStateException if the active span isn't an exit one. Ref to {@link
     * AbstractTracerContext#inject(ContextCarrier)}
     */
    @Override
    public void inject(ContextCarrier carrier) {
        AbstractSpan span = this.activeSpan();
        if (!span.isExit()) {
            throw new IllegalStateException("Inject can be done only in Exit Span");
        }
        // 检测当前活跃的 Span是否为 ExitSpan，否则会抛出异常(略)
        WithPeerInfo spanWithPeer = (WithPeerInfo)span;
        String peer = spanWithPeer.getPeer();
        int peerId = spanWithPeer.getPeerId();
        // 设置 TraceSegmentId
        carrier.setTraceSegmentId(this.segment.getTraceSegmentId());
        carrier.setSpanId(span.getSpanId()); // 设置 SpanId
        // 设置 service_instance_id
        carrier.setParentServiceInstanceId(segment.getApplicationInstanceId());

        if (DictionaryUtil.isNull(peerId)) {
            carrier.setPeerHost(peer);
        } else {
            carrier.setPeerId(peerId);
        }
        List<TraceSegmentRef> refs = this.segment.getRefs();
        int operationId;
        String operationName;
        int entryApplicationInstanceId;
        if (refs != null && refs.size() > 0) {
            TraceSegmentRef ref = refs.get(0);
            operationId = ref.getEntryEndpointId();
            operationName = ref.getEntryEndpointName();
            entryApplicationInstanceId = ref.getEntryServiceInstanceId();
        } else {
            AbstractSpan firstSpan = first();
            operationId = firstSpan.getOperationId();
            operationName = firstSpan.getOperationName();
            entryApplicationInstanceId = this.segment.getApplicationInstanceId();
        } // 设置 entryApplicationInstanceId
        carrier.setEntryServiceInstanceId(entryApplicationInstanceId);
        // 设置 operationName或 operationId
        if (operationId == DictionaryUtil.nullValue()) {
            if (!StringUtil.isEmpty(operationName)) {
                carrier.setEntryEndpointName(operationName);
            }
        } else {
            carrier.setEntryEndpointId(operationId);
        }
        // 设置 parentEndpointId或是 parentEndpointName
        int parentOperationId = first().getOperationId();
        if (parentOperationId == DictionaryUtil.nullValue()) {
            carrier.setParentEndpointName(first().getOperationName());
        } else {
            carrier.setParentEndpointId(parentOperationId);
        }
        // 设置 primaryDistributedTraceId，只会从 relatedGlobalTraces集合中取第一个
        carrier.setDistributedTraceIds(this.segment.getRelatedGlobalTraces());
    }

    /**
     * Extract the carrier to build the reference for the pre segment.
     *
     * @param carrier carried the context from a cross-process segment. Ref to {@link
     * AbstractTracerContext#extract(ContextCarrier)}
     */
    @Override
    public void extract(ContextCarrier carrier) {
        TraceSegmentRef ref = new TraceSegmentRef(carrier);
        this.segment.ref(ref);
        this.segment.relatedGlobalTraces(carrier.getDistributedTraceId());
        AbstractSpan span = this.activeSpan();
        if (span instanceof EntrySpan) {
            span.ref(ref);
        }
    }

    /**
     * Capture the snapshot of current context.
     *
     * @return the snapshot of context for cross-thread propagation Ref to {@link AbstractTracerContext#capture()}
     */
    @Override
    public ContextSnapshot capture() {
        List<TraceSegmentRef> refs = this.segment.getRefs();
        ContextSnapshot snapshot = new ContextSnapshot(segment.getTraceSegmentId(),
            activeSpan().getSpanId(),
            segment.getRelatedGlobalTraces());
        int entryOperationId;
        String entryOperationName;
        int entryApplicationInstanceId;
        AbstractSpan firstSpan = first();
        if (refs != null && refs.size() > 0) {
            TraceSegmentRef ref = refs.get(0);
            entryOperationId = ref.getEntryEndpointId();
            entryOperationName = ref.getEntryEndpointName();
            entryApplicationInstanceId = ref.getEntryServiceInstanceId();
        } else {
            entryOperationId = firstSpan.getOperationId();
            entryOperationName = firstSpan.getOperationName();
            entryApplicationInstanceId = this.segment.getApplicationInstanceId();
        }
        snapshot.setEntryApplicationInstanceId(entryApplicationInstanceId);

        if (entryOperationId == DictionaryUtil.nullValue()) {
            if (!StringUtil.isEmpty(entryOperationName)) {
                snapshot.setEntryOperationName(entryOperationName);
            }
        } else {
            snapshot.setEntryOperationId(entryOperationId);
        }

        if (firstSpan.getOperationId() == DictionaryUtil.nullValue()) {
            snapshot.setParentOperationName(firstSpan.getOperationName());
        } else {
            snapshot.setParentOperationId(firstSpan.getOperationId());
        }
        return snapshot;
    }

    /**
     * Continue the context from the given snapshot of parent thread.
     *
     * @param snapshot from {@link #capture()} in the parent thread. Ref to {@link AbstractTracerContext#continued(ContextSnapshot)}
     */
    @Override
    public void continued(ContextSnapshot snapshot) {
        TraceSegmentRef segmentRef = new TraceSegmentRef(snapshot);
        this.segment.ref(segmentRef);
        this.activeSpan().ref(segmentRef);
        this.segment.relatedGlobalTraces(snapshot.getDistributedTraceId());
    }

    /**
     * @return the first global trace id.
     */
    @Override
    public String getReadableGlobalTraceId() {
        return segment.getRelatedGlobalTraces().get(0).toString();
    }

    /**
     * Create an entry span
     *
     * @param operationName most likely a service name
     * @return span instance. Ref to {@link EntrySpan}
     */
    @Override
    public AbstractSpan createEntrySpan(final String operationName) {
        if (isLimitMechanismWorking()) { // 默认，每个 TraceSegment只能放300个 Span，超过了就每30s打一条 WARN日志
            NoopSpan span = new NoopSpan(); // 超过300就放 NoopSpan
            return push(span);
        }
        AbstractSpan entrySpan;
        final AbstractSpan parentSpan = peek(); // 获取 activeSpanStack集合中的最后一个 Span，就是父Span
        final int parentSpanId = parentSpan == null ? -1 : parentSpan.getSpanId();
        if (parentSpan != null && parentSpan.isEntry()) {
            entrySpan = (AbstractTracingSpan)DictionaryManager.findEndpointSection()
                .findOnly(segment.getServiceId(), operationName)
                .doInCondition(new PossibleFound.FoundAndObtain() {
                    @Override public Object doProcess(int operationId) {
                        return parentSpan.setOperationId(operationId);
                    }
                }, new PossibleFound.NotFoundAndObtain() {
                    @Override public Object doProcess() {
                        return parentSpan.setOperationName(operationName);
                    }
                });// 更新 operationId(或operationName)，然后重新 start(前面提到过，start()方法会重置其他字段)
            return entrySpan.start();
        } else {
            entrySpan = (AbstractTracingSpan)DictionaryManager.findEndpointSection()
                .findOnly(segment.getServiceId(), operationName)
                .doInCondition(new PossibleFound.FoundAndObtain() {
                    @Override public Object doProcess(int operationId) {
                        return new EntrySpan(spanIdGenerator++, parentSpanId, operationId);
                    }
                }, new PossibleFound.NotFoundAndObtain() {
                    @Override public Object doProcess() {
                        return new EntrySpan(spanIdGenerator++, parentSpanId, operationName);
                    }
                }); // 新建 EntrySpan对象，并调用 start()方法
            entrySpan.start();
            return push(entrySpan);
        }
    }

    /**
     * Create a local span
     *
     * @param operationName most likely a local method signature, or business name.
     * @return the span represents a local logic block. Ref to {@link LocalSpan}
     */
    @Override
    public AbstractSpan createLocalSpan(final String operationName) {
        if (isLimitMechanismWorking()) {
            NoopSpan span = new NoopSpan();
            return push(span);
        }// 检测当前 TraceSegment中的 Span个数
        AbstractSpan parentSpan = peek(); // 获取父 Span以及父 Span的Id
        final int parentSpanId = parentSpan == null ? -1 : parentSpan.getSpanId();
        /**
         * From v6.0.0-beta, local span doesn't do op name register.
         * All op name register is related to entry and exit spans only.
         */ // 直接创建 LocalSpan对象并调用其 start()方法
        AbstractTracingSpan span = new LocalSpan(spanIdGenerator++, parentSpanId, operationName);
        span.start();
        return push(span);
    }

    /**
     * Create an exit span
     *
     * @param operationName most likely a service name of remote
     * @param remotePeer the network id(ip:port, hostname:port or ip1:port1,ip2,port, etc.)
     * @return the span represent an exit point of this segment.
     * @see ExitSpan
     */
    @Override
    public AbstractSpan createExitSpan(final String operationName, final String remotePeer) {
        AbstractSpan exitSpan;
        AbstractSpan parentSpan = peek();
        if (parentSpan != null && parentSpan.isExit()) {
            exitSpan = parentSpan;
        } else {
            final int parentSpanId = parentSpan == null ? -1 : parentSpan.getSpanId();
            exitSpan = (AbstractSpan)DictionaryManager.findNetworkAddressSection()
                .find(remotePeer).doInCondition(
                    new PossibleFound.FoundAndObtain() {
                        @Override
                        public Object doProcess(final int peerId) {
                            if (isLimitMechanismWorking()) {
                                return new NoopExitSpan(peerId);
                            }

                            return DictionaryManager.findEndpointSection()
                                .findOnly(segment.getServiceId(), operationName)
                                .doInCondition(
                                    new PossibleFound.FoundAndObtain() {
                                        @Override
                                        public Object doProcess(int operationId) {
                                            return new ExitSpan(spanIdGenerator++, parentSpanId, operationId, peerId);
                                        }
                                    }, new PossibleFound.NotFoundAndObtain() {
                                        @Override
                                        public Object doProcess() {
                                            return new ExitSpan(spanIdGenerator++, parentSpanId, operationName, peerId);
                                        }
                                    });
                        }
                    },
                    new PossibleFound.NotFoundAndObtain() {
                        @Override
                        public Object doProcess() {
                            if (isLimitMechanismWorking()) {
                                return new NoopExitSpan(remotePeer);
                            }

                            return DictionaryManager.findEndpointSection()
                                .findOnly(segment.getServiceId(), operationName)
                                .doInCondition(
                                    new PossibleFound.FoundAndObtain() {
                                        @Override
                                        public Object doProcess(int operationId) {
                                            return new ExitSpan(spanIdGenerator++, parentSpanId, operationId, remotePeer);
                                        }
                                    }, new PossibleFound.NotFoundAndObtain() {
                                        @Override
                                        public Object doProcess() {
                                            return new ExitSpan(spanIdGenerator++, parentSpanId, operationName, remotePeer);
                                        }
                                    });
                        }
                    });
            push(exitSpan);
        }
        exitSpan.start();
        return exitSpan;
    }

    /**
     * @return the active span of current context, the top element of {@link #activeSpanStack}
     */
    @Override
    public AbstractSpan activeSpan() {
        AbstractSpan span = peek();
        if (span == null) {
            throw new IllegalStateException("No active span.");
        }
        return span;
    }

    /**
     * Stop the given span, if and only if this one is the top element of {@link #activeSpanStack}. Because the tracing
     * core must make sure the span must match in a stack module, like any program did.
     *
     * @param span to finish
     */
    @Override
    public boolean stopSpan(AbstractSpan span) {
        AbstractSpan lastSpan = peek(); // 获取当前活跃的 Span对象
        if (lastSpan == span) { // 只能关闭当前活跃 Span对象，否则抛异常
            if (lastSpan instanceof AbstractTracingSpan) {
                AbstractTracingSpan toFinishSpan = (AbstractTracingSpan)lastSpan;
                if (toFinishSpan.finish(segment)) { // 尝试关闭 Span，当完全关闭之后，会将其从 activeSpanStack集合中删除
                    pop();
                }
            } else {
                pop();
            }
        } else {
            throw new IllegalStateException("Stopping the unexpected span = " + span);
        }
        // TraceSegment中全部 Span都关闭(且异步状态的 Span也关闭了)，则当前 TraceSegment也会关闭
        if (checkFinishConditions()) {
            finish();
        }

        return activeSpanStack.isEmpty();
    }

    @Override public AbstractTracerContext awaitFinishAsync() {
        if (!isRunningInAsyncMode) {
            synchronized (this) {
                if (!isRunningInAsyncMode) {
                    asyncFinishLock = new ReentrantLock();
                    asyncSpanCounter = new AtomicInteger(0);
                    isRunningInAsyncMode = true;
                }
            }
        }
        asyncSpanCounter.addAndGet(1);
        return this;
    }

    @Override public void asyncStop(AsyncSpan span) {
        asyncSpanCounter.addAndGet(-1);

        if (checkFinishConditions()) {
            finish();
        }
    }

    private boolean checkFinishConditions() {
        if (isRunningInAsyncMode) {
            asyncFinishLock.lock();
        }
        try {
            if (activeSpanStack.isEmpty() && (!isRunningInAsyncMode || asyncSpanCounter.get() == 0)) {
                return true;
            }
        } finally {
            if (isRunningInAsyncMode) {
                asyncFinishLock.unlock();
            }
        }
        return false;
    }

    /**
     * Finish this context, and notify all {@link TracingContextListener}s, managed by {@link
     * TracingContext.ListenerManager}
     */
    private void finish() {
        TraceSegment finishedSegment = segment.finish(isLimitMechanismWorking());
        /**
         * Recheck the segment if the segment contains only one span.
         * Because in the runtime, can't sure this segment is part of distributed trace.
         *
         * @see {@link #createSpan(String, long, boolean)}
         */
        if (!segment.hasRef() && segment.isSingleSpanSegment()) {
            if (!samplingService.trySampling()) {
                finishedSegment.setIgnore(true);
            }
        }
        TracingContext.ListenerManager.notifyFinish(finishedSegment);
    }

    /**
     * The <code>ListenerManager</code> represents an event notify for every registered listener, which are notified
     * when the <code>TracingContext</code> finished, and {@link #segment} is ready for further process.
     */
    public static class ListenerManager {
        private static List<TracingContextListener> LISTENERS = new LinkedList<TracingContextListener>();

        /**
         * Add the given {@link TracingContextListener} to {@link #LISTENERS} list.
         *
         * @param listener the new listener.
         */
        public static synchronized void add(TracingContextListener listener) {
            LISTENERS.add(listener);
        }

        /**
         * Notify the {@link TracingContext.ListenerManager} about the given {@link TraceSegment} have finished. And
         * trigger {@link TracingContext.ListenerManager} to notify all {@link #LISTENERS} 's {@link
         * TracingContextListener#afterFinished(TraceSegment)}
         *
         * @param finishedSegment
         */
        static void notifyFinish(TraceSegment finishedSegment) {
            for (TracingContextListener listener : LISTENERS) {
                listener.afterFinished(finishedSegment);
            }
        }

        /**
         * Clear the given {@link TracingContextListener}
         */
        public static synchronized void remove(TracingContextListener listener) {
            LISTENERS.remove(listener);
        }

    }

    /**
     * @return the top element of 'ActiveSpanStack', and remove it.
     */
    private AbstractSpan pop() {
        return activeSpanStack.removeLast();
    }

    /**
     * Add a new Span at the top of 'ActiveSpanStack'
     *
     * @param span
     */
    private AbstractSpan push(AbstractSpan span) {
        activeSpanStack.addLast(span);
        return span;
    }

    /**
     * @return the top element of 'ActiveSpanStack' only.
     */
    private AbstractSpan peek() {
        if (activeSpanStack.isEmpty()) {
            return null;
        }
        return activeSpanStack.getLast();
    }

    private AbstractSpan first() {
        return activeSpanStack.getFirst();
    }

    private boolean isLimitMechanismWorking() {
        if (spanIdGenerator >= Config.Agent.SPAN_LIMIT_PER_SEGMENT) {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastWarningTimestamp > 30 * 1000) {
                logger.warn(new RuntimeException("Shadow tracing context. Thread dump"), "More than {} spans required to create",
                    Config.Agent.SPAN_LIMIT_PER_SEGMENT);
                lastWarningTimestamp = currentTimeMillis;
            }
            return true;
        } else {
            return false;
        }
    }
}
