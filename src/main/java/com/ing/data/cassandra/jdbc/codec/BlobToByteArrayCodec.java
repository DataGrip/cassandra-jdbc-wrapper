package com.ing.data.cassandra.jdbc.codec;


import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class BlobToByteArrayCodec implements TypeCodec<byte[]> {
    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.BLOB;
    }

    @Nonnull
    @Override
    public GenericType<byte[]> getJavaType() {
        return GenericType.of(byte[].class);
    }

    @Override
    public ByteBuffer encode(byte[] value, @Nonnull ProtocolVersion protocolVersion) {
        return value == null ? null : ByteBuffer.wrap(value);
    }

    @Override
    public byte[] decode(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        return bytes == null ? null : Bytes.getArray(bytes);
    }

    @Override
    public byte[] parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                ? null
                : Bytes.getArray(Bytes.fromHexString(value));
    }

    @Override
    @Nonnull
    public String format(byte[] value) {
        if (value == null) return "NULL";
        return Bytes.toHexString(value);
    }
}
