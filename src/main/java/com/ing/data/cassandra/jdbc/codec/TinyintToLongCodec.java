package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class TinyintToLongCodec extends BaseLongCodec {

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.TINYINT;
    }

    @Override
    int getNumberOfBytes() {
        return 1;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) {
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new IllegalArgumentException("Long value " + value + " does not fit into cql type tinyint");
        }
        bb.put((byte) value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return bytes.get();
    }
}
