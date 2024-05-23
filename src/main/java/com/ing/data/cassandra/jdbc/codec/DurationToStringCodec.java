package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.Duration;

/**
 * @author Liudmila Kornilova
 **/
public class DurationToStringCodec implements TypeCodec<String> {
    private final TypeCodec<Duration> durationCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.DURATION, Duration.class);

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.DURATION;
    }

    @NonNull
    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public ByteBuffer encode(String value, @NonNull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        Duration duration = Duration.parse(value);
        return durationCodec.encode(duration, protocolVersion);
    }

    @Override
    public String decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        Duration duration = durationCodec.decode(bytes, protocolVersion);
        return duration == null ? null : duration.toString();
    }

    @Override
    @Nullable
    public String parse(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }

    @Override
    @NonNull
    public String format(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }
}
