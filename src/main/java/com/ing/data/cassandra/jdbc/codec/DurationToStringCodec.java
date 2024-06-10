package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class DurationToStringCodec implements TypeCodec<String> {
    private final TypeCodec<CqlDuration> durationCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.DURATION, CqlDuration.class);

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.DURATION;
    }

    @Nonnull
    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public ByteBuffer encode(String value, @Nonnull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        CqlDuration duration = CqlDuration.from(value);
        return durationCodec.encode(duration, protocolVersion);
    }

    @Override
    public String decode(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        CqlDuration duration = durationCodec.decode(bytes, protocolVersion);
        return duration == null ? null : duration.toString();
    }

    @Override
    @Nullable
    public String parse(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }

    @Override
    @Nonnull
    public String format(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }
}
