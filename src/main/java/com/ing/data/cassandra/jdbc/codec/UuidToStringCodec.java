package com.ing.data.cassandra.jdbc.codec;


import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Liudmila Kornilova
 **/
public class UuidToStringCodec implements TypeCodec<String> {
    private final TypeCodec<UUID> uuidCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.UUID, UUID.class);

    @Nonnull
    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.UUID;
    }

    @Nullable
    @Override
    public ByteBuffer encode(@Nullable String value, @Nonnull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        UUID uuid = UUID.fromString(value);
        return uuidCodec.encode(uuid, protocolVersion);
    }

    @Nullable
    @Override
    public String decode(@Nullable ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        UUID decode = uuidCodec.decode(bytes, protocolVersion);
        return decode == null ? null : decode.toString();
    }

    @Override
    public String parse(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }

    @Override
    @Nonnull
    public String format(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }
}
