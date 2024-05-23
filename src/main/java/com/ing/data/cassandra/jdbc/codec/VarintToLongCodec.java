package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class VarintToLongCodec implements TypeCodec<Long> {

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.VARINT;
    }

    @NonNull
    @Override
    public GenericType<Long> getJavaType() {
        return GenericType.LONG;
    }

    @Override
    public Long parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
            ? null
            : Long.valueOf(value);
    }

    @Override
    @NonNull
    public String format(Long value) {
        if (value == null) return "NULL";
        return value.toString();
    }

    @Override
    public ByteBuffer encode(Long value, @NonNull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        BigInteger bigInteger = new BigInteger(value.toString());
        return ByteBuffer.wrap(bigInteger.toByteArray());
    }

    @Override
    public Long decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : new BigInteger(Bytes.getArray(bytes)).longValue();
    }
}
