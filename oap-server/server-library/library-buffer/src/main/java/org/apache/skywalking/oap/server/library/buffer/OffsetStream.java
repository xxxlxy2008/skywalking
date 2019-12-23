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

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.*;
import lombok.Getter;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.slf4j.*;

/**
 * @author peng-yongsheng
 */
class OffsetStream {

    private static final Logger logger = LoggerFactory.getLogger(OffsetStream.class);

    private final File directory;
    private final int offsetFileMaxSize; // offset文件的大小上限值，单位为MB

    @Getter private final Offset offset; // 其中封装了ReadOffset和WriteOffset，分别用来用来记录读写的文件名和读写的偏移量
    private File offsetFile;
    private boolean initialized = false;
    private String lastOffsetRecord = "";

    OffsetStream(File directory, int offsetFileMaxSize) {
        this.directory = directory;
        this.offsetFileMaxSize = offsetFileMaxSize;
        this.offset = new Offset();
    }

    void clean() throws IOException {
        String[] fileNames = directory.list(new PrefixFileFilter(BufferFileUtils.OFFSET_FILE_PREFIX));
        if (fileNames != null) {
            for (String fileName : fileNames) {
                File file = new File(directory, fileName);
                if (logger.isDebugEnabled()) {
                    logger.debug("Delete buffer offset file: {}", file.getAbsolutePath());
                }
                FileUtils.forceDelete(new File(directory, fileName));
            }
        }
    }

    synchronized void initialize() throws IOException {
        if (!initialized) {
            // 读取 offset文件，offset文件名格式是：offset-时间戳.sw
            String[] fileNames = directory.list(new PrefixFileFilter(BufferFileUtils.OFFSET_FILE_PREFIX));
            if (fileNames != null && fileNames.length > 0) {
                BufferFileUtils.sort(fileNames); // 排序，读取最新一个offset
                offsetFile = new File(directory, fileNames[0]);
            } else {
                offsetFile = newFile(); // 新建文件
            }
            // 直接读取最新offset文件的最后一行。offset文件中，每一行数据的格式分为：read文件名，read文件offset，write文件名，write文件offset
            offset.deserialize(readLastLine());
            initialized = true;

            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                new RunnableWithExceptionProtection(this::flush,
                    t -> logger.error("Flush offset file in background failure.", t)
                ), 2, 1, TimeUnit.SECONDS);
        }
    }

    void flush() {
        try {
            String offsetRecord = offset.serialize(); // 序列化Offset
            if (!lastOffsetRecord.equals(offsetRecord)) { // 检测lastOffsetRecord是否发生变化
                logger.debug("flush offset, record: {}", offsetRecord);
                if (offsetFile.length() >= FileUtils.ONE_MB * offsetFileMaxSize) {
                    nextFile();
                }
                // 将lastOffsetRecord写入到offset文件
                try (OutputStream out = new BufferedOutputStream(FileUtils.openOutputStream(offsetFile, true))) {
                    IOUtils.write(offsetRecord, out, Charset.forName(BufferFileUtils.CHARSET));
                    IOUtils.write(System.lineSeparator(), out, Charset.forName(BufferFileUtils.CHARSET));
                }
                lastOffsetRecord = offsetRecord; // 更新lastOffsetRecord
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void nextFile() throws IOException {
        File newOffsetFile = newFile(); // 新建offset文件
        if (!offsetFile.delete()) { // 尝试删除原有offset文件
            logger.warn("Offset file {} delete failure.", newOffsetFile.getAbsolutePath());
        }
        offsetFile = newOffsetFile;
        this.flush();
    }

    private File newFile() throws IOException {
        String fileName = BufferFileUtils.buildFileName(BufferFileUtils.OFFSET_FILE_PREFIX);
        File file = new File(directory, fileName);
        if (file.createNewFile()) {
            logger.info("Create a new offset file {}", fileName);
        }
        return file;
    }

    private String readLastLine() throws IOException {
        ReversedLinesFileReader reader = new ReversedLinesFileReader(offsetFile, Charset.forName(BufferFileUtils.CHARSET));
        return reader.readLine();
    }
}
