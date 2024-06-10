package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveLongCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public abstract class BaseLongCodec implements PrimitiveLongCodec {
    abstract int getNumberOfBytes();

    abstract void serializeNoBoxingInner(long value, ByteBuffer bb);

    abstract long deserializeNoBoxingInner(ByteBuffer bytes);

    @Nonnull
    @Override
    public GenericType<Long> getJavaType() {
        return GenericType.LONG;
    }

    @Override
    public Long parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : Long.parseLong(value);
    }

    @Override
    @Nonnull
    public String format(@Nullable Long value) {
        if (value == null) return "NULL";
        return Long.toString(value);
    }

    @Override
    public ByteBuffer encodePrimitive(long value, @Nonnull ProtocolVersion protocolVersion) {
        ByteBuffer bb = ByteBuffer.allocate(getNumberOfBytes());
        serializeNoBoxingInner(value, bb);
        bb.flip();
        return bb;
    }

    @Override
    public long decodePrimitive(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0) return 0;
        if (bytes.remaining() != getNumberOfBytes()) {
            throw new IllegalStateException("Invalid value, expecting " + getNumberOfBytes() + " bytes but got " + bytes.remaining());
        }

        return deserializeNoBoxingInner(bytes);
    }
}
