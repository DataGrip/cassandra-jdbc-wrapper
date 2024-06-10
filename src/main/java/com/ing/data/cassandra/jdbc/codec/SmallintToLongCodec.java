package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class SmallintToLongCodec extends BaseLongCodec {
    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.SMALLINT;
    }

    @Override
    int getNumberOfBytes() {
        return 2;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) {
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new IllegalArgumentException("Long value " + value + " does not fit into cql type smallint");
        }
        bb.putShort((short) value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return bytes.getShort();
    }
}
