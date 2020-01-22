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

package org.apache.skywalking.oap.server.library.buffer;

import com.google.protobuf.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.skywalking.apm.util.*;
import org.slf4j.*;

/**
 * @author peng-yongsheng
 */
public class DataStreamReader<MESSAGE_TYPE extends GeneratedMessageV3> {

    private static final Logger logger = LoggerFactory.getLogger(DataStreamReader.class);
    // data文件所在的文件夹
    private final File directory;
    // ReadOffset中记录了DataStreamReader当前读取的data文件名以及偏移量
    private final Offset.ReadOffset readOffset;
    private final Parser<MESSAGE_TYPE> parser;
    private final int collectionSize = 100;
    // 用于缓存读取到的 UpstreamSegment
    private final BufferDataCollection<MESSAGE_TYPE> bufferDataCollection;
    // 指向当前读取的文件以及对应的文件流
    private File readingFile;
    private InputStream inputStream;
    // 指向 SegmentParseV2.Producer，即用于重新解析 UpstreamSegment
    private final CallBack<MESSAGE_TYPE> callBack;

    DataStreamReader(File directory, Offset.ReadOffset readOffset, Parser<MESSAGE_TYPE> parser,
        CallBack<MESSAGE_TYPE> callBack) {
        this.directory = directory;
        this.readOffset = readOffset;
        this.parser = parser;
        this.callBack = callBack;
        this.bufferDataCollection = new BufferDataCollection<>(collectionSize);
    }

    void initialize() {
        preRead();

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
            new RunnableWithExceptionProtection(this::read,
                t -> logger.error("Buffer data pre read failure.", t)), 3, 1, TimeUnit.SECONDS);
    }

    private void preRead() {
        String fileName = readOffset.getFileName();
        if (StringUtil.isEmpty(fileName)) {
            openInputStream(readEarliestDataFile());
        } else {
            File readingFile = new File(directory, fileName);
            if (readingFile.exists()) {
                openInputStream(readingFile);
                try {
                    inputStream.skip(readOffset.getOffset());
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            } else {
                openInputStream(readEarliestDataFile());
            }
        }
    }

    private void openInputStream(File readingFile) {
        try {
            this.readingFile = readingFile;
            if (Objects.nonNull(inputStream)) {
                inputStream.close();
            }

            inputStream = new FileInputStream(readingFile);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private File readEarliestDataFile() {
        String[] fileNames = directory.list(new PrefixFileFilter(BufferFileUtils.DATA_FILE_PREFIX));

        if (fileNames != null && fileNames.length > 0) {
            BufferFileUtils.sort(fileNames);
            readOffset.setFileName(fileNames[0]);
            readOffset.setOffset(0);
            return new File(directory, fileNames[0]);
        } else {
            return null;
        }
    }

    private void read() {
        if (logger.isDebugEnabled()) {
            logger.debug("Read buffer data");
        }

        try {
            if (readOffset.getOffset() == readingFile.length() && !readOffset.isCurrentWriteFile()) {
                FileUtils.forceDelete(readingFile);
                openInputStream(readEarliestDataFile());
            }

            while (readOffset.getOffset() < readingFile.length()) {
                // 读取 data文件，反序列化得到 UpstreamSegment对象，并封装成 BufferData
                BufferData<MESSAGE_TYPE> bufferData =
                        new BufferData<>(parser.parseDelimitedFrom(inputStream));

                if (bufferData.getMessageType() != null) {
                    // 将 BufferData直接交给SegmentParser处理，整个解析逻辑在前面介绍过了，
                    // 唯一区别在于：这里传入的SegmentSource值不是 Agent，而是Buffer
                    boolean isComplete = callBack.call(bufferData);
                    // 重新计算文件的偏移量，并记录到 readOffset.offset字段中
                    final int serialized = bufferData.getMessageType().getSerializedSize();
                    final int offset = CodedOutputStream.computeUInt32SizeNoTag(serialized) + serialized;
                    readOffset.setOffset(readOffset.getOffset() + offset);

                    if (!isComplete) { // 上面的callback.call() 再次失败，则缓存到bufferDataCollection集合中
                        if (bufferDataCollection.size() == collectionSize) {
                            reCall(); // 再次尝试解析bufferDataCollection集合中的 UpstreamSegment
                        }
                        bufferDataCollection.add(bufferData);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("collection size: {}, max size: {}", bufferDataCollection.size(), collectionSize);
                    }
                } else if (bufferDataCollection.size() > 0) {
                    reCall();// 未读取到新的UpstreamSegment，再次尝试处理bufferDataCollection集合
                } else { //
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void reCall() {
        int maxCycle = 10;
        for (int i = 1; i <= maxCycle; i++) { // 尝试10次
            if (bufferDataCollection.size() > 0) {
                List<BufferData<MESSAGE_TYPE>> bufferDataList = bufferDataCollection.export();
                for (BufferData<MESSAGE_TYPE> data : bufferDataList) {
                    if (!callBack.call(data)) {
                        if (i != maxCycle) {
                            bufferDataCollection.add(data);
                        }
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            } else {
                break;
            }
        }
    }

    public interface CallBack<MESSAGE_TYPE extends GeneratedMessageV3> {
        boolean call(BufferData<MESSAGE_TYPE> bufferData);
    }
}
