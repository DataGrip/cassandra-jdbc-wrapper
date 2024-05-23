package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class DoubleToLongCodec extends BaseLongCodec {

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.DOUBLE;
    }

    @Override
    int getNumberOfBytes() {
        return 8;
    }

    @Override
    void serializeNoBoxingInner(long value, ByteBuffer bb) {
        bb.putDouble(value);
    }

    @Override
    long deserializeNoBoxingInner(ByteBuffer bytes) {
        return (long) bytes.getDouble();
    }
}
