package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class FloatToLongCodec extends BaseLongCodec {
    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.FLOAT;
    }

    @Override
    int getNumberOfBytes() {
        return 4;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) {
        bb.putFloat(value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return (long) bytes.getFloat();
    }
}
