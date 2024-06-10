package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Liudmila Kornilova
 **/
public class TimeuuidToStringCodec implements TypeCodec<String> {
    private final TypeCodec<UUID> timeuuidCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.TIMEUUID, UUID.class);

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.TIMEUUID;
    }

    @Nonnull
    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public ByteBuffer encode(String value, @Nonnull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        return timeuuidCodec.encode(UUID.fromString(value), protocolVersion);
    }

    @Override
    public String decode(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        UUID uuid = timeuuidCodec.decode(bytes, protocolVersion);
        return uuid == null ? null : uuid.toString();
    }

    @Override
    public String parse(String value) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    @Nonnull
    public String format(String value) {
        throw new RuntimeException("Not implemented");
    }
}
