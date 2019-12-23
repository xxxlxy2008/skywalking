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

package org.apache.skywalking.oap.server.core.analysis.data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author peng-yongsheng
 */
public abstract class Window<DATA> {
    // 用于控制当前是否
    private AtomicInteger windowSwitch = new AtomicInteger(0);

    private SWCollection<DATA> pointer;

    private SWCollection<DATA> windowDataA;
    private SWCollection<DATA> windowDataB;

    Window() {
        this(true);
    }

    Window(boolean autoInit) {
        if (autoInit) {
            init();
        }
    }

    protected void init() {
        this.windowDataA = collectionInstance();
        this.windowDataB = collectionInstance();
        this.pointer = windowDataA;
    }

    public abstract SWCollection<DATA> collectionInstance();

    public boolean trySwitchPointer() {
        // 检查 windowSwitch字段，以及last队列是否处于可读状态
        return windowSwitch.incrementAndGet() == 1 && !getLast().isReading();
    }

    public void trySwitchPointerFinally() {
        // 在 trySwitchPointer()方法尝试之后，需要在finally代码块中恢复windowSwitch字段的值，为下次检查做准备
        windowSwitch.addAndGet(-1);
    }

    public void switchPointer() {
        // 根据 pointer当前的指向，进行修改
        if (pointer == windowDataA) {
            pointer = windowDataB;
        } else {
            pointer = windowDataA;
        }
        getLast().reading(); // 修改 last队列的状态
    }

    SWCollection<DATA> getCurrentAndWriting() {
        if (pointer == windowDataA) {
            windowDataA.writing();
            return windowDataA;
        } else {
            windowDataB.writing();
            return windowDataB;
        }
    }

    private SWCollection<DATA> getCurrent() {
        return pointer;
    }

    public int currentCollectionSize() {
        return getCurrent().size();
    }

    public SWCollection<DATA> getLast() {
        if (pointer == windowDataA) {
            return windowDataB;
        } else {
            return windowDataA;
        }
    }

    public void finishReadingLast() {
        getLast().clear();
        getLast().finishReading();
    }
}
