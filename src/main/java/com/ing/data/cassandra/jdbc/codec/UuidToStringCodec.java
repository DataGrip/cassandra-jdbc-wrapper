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
import java.util.UUID;

/**
 * @author Liudmila Kornilova
 **/
public class UuidToStringCodec implements TypeCodec<String> {
    private final TypeCodec<UUID> uuidCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.UUID, UUID.class);

    @NonNull
    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.UUID;
    }

    @Nullable
    @Override
    public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        UUID uuid = UUID.fromString(value);
        return uuidCodec.encode(uuid, protocolVersion);
    }

    @Nullable
    @Override
    public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        UUID decode = uuidCodec.decode(bytes, protocolVersion);
        return decode == null ? null : decode.toString();
    }

    @Override
    public String parse(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }

    @Override
    @NonNull
    public String format(@Nullable String value) {
        throw new RuntimeException("Not supported");
    }
}
