package com.ing.data.cassandra.jdbc.codec;


import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class DecimalToLongCodec implements TypeCodec<Long> {
    @Nonnull
    @Override
    public GenericType<Long> getJavaType() {
        return GenericType.LONG;
    }

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.DECIMAL;
    }

    @Override
    public Long parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
            ? null
            : Long.valueOf(value);
    }

    @Override
    @Nonnull
    public String format(Long value) {
        if (value == null) return "NULL";
        return value.toString();
    }

    @Override
    public ByteBuffer encode(Long value, @Nonnull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        BigDecimal bigDecimal = new BigDecimal(value);
        BigInteger bi = bigDecimal.unscaledValue();
        int scale = bigDecimal.scale();
        byte[] bibytes = bi.toByteArray();

        ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
        bytes.putInt(scale);
        bytes.put(bibytes);
        bytes.rewind();
        return bytes;
    }

    @Override
    public Long decode(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0) return null;
        if (bytes.remaining() < 4) {
            throw new IllegalStateException("Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());
        }

        bytes = bytes.duplicate();
        int scale = bytes.getInt();
        byte[] bibytes = new byte[bytes.remaining()];
        bytes.get(bibytes);

        BigInteger bi = new BigInteger(bibytes);
        return new BigDecimal(bi, scale).longValue();
    }
}
