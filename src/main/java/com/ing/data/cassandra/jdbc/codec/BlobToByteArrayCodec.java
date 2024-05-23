package com.ing.data.cassandra.jdbc.codec;


import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class BlobToByteArrayCodec implements TypeCodec<byte[]> {
    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.BLOB;
    }

    @NonNull
    @Override
    public GenericType<byte[]> getJavaType() {
        return GenericType.of(byte[].class);
    }

    @Override
    public ByteBuffer encode(byte[] value, @NonNull ProtocolVersion protocolVersion) {
        return value == null ? null : ByteBuffer.wrap(value);
    }

    @Override
    public byte[] decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        return bytes == null ? null : Bytes.getArray(bytes);
    }

    @Override
    public byte[] parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                ? null
                : Bytes.getArray(Bytes.fromHexString(value));
    }

    @Override
    @NonNull
    public String format(byte[] value) {
        if (value == null) return "NULL";
        return Bytes.toHexString(value);
    }
}
