package org.apache.rocketmq.delay.store.model;

import java.nio.ByteBuffer;


public abstract class AbstractLogRecord implements LogRecord{

    @Override
    public String getSubject() {
        return null;
    }

    @Override
    public long getScheduleTime() {
        return 0;
    }

    @Override
    public int getPayloadSize() {
        return 0;
    }

    @Override
    public ByteBuffer getBody() {
        return null;
    }

    @Override
    public long getStartOffset() {
        return 0;
    }


    @Override
    public int getFullSize() {
        return 0;
    }
}
