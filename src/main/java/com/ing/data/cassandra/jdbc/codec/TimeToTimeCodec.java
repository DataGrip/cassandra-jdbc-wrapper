package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.sql.Time;

/**
 * @author Liudmila Kornilova
 **/
public class TimeToTimeCodec implements TypeCodec<Time> {
    private final TypeCodec<Long> timeCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.TIME, Long.class);

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.TIME;
    }

    @Nonnull
    @Override
    public GenericType<Time> getJavaType() {
        return GenericType.of(Time.class);
    }

    @Override
    public ByteBuffer encode(Time value, @Nonnull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        long milliseconds = value.getTime();
        return timeCodec.encode(milliseconds * 1000000, protocolVersion);
    }

    @Override
    public Time decode(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        Long nanoseconds = timeCodec.decode(bytes, protocolVersion);
        return nanoseconds == null ? null : new Time(nanoseconds / 1000000);
    }

    @Override
    public Time parse(String value) {
        throw new RuntimeException("Not supported");
    }

    @Override
    @Nonnull
    public String format(Time value) {
        throw new RuntimeException("Not supported");
    }
}
