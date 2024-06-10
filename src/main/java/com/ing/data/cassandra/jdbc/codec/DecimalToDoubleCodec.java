package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveDoubleCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.nio.ByteBuffer;


public class DecimalToDoubleCodec extends AbstractCodec<Double> implements PrimitiveDoubleCodec {
    private final TypeCodec<BigDecimal> decimalCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.DECIMAL, BigDecimal.class);

    @Nonnull
    @Override
    public DataType getCqlType() {
        return DataTypes.DECIMAL;
    }

    @Nonnull
    @Override
    public GenericType<Double> getJavaType() {
        return GenericType.DOUBLE;
    }

    @Override
    public ByteBuffer encodePrimitive(double value, @Nonnull ProtocolVersion protocolVersion) {
        return decimalCodec.encode(BigDecimal.valueOf(value), protocolVersion);
    }

    @Override
    public double decodePrimitive(ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
        BigDecimal bigDecimal = decimalCodec.decode(bytes, protocolVersion);
        if (bigDecimal == null) return 0;
        return bigDecimal.doubleValue();
    }


    @Override
    Double parseNonNull(@Nonnull final String value) {
        return Double.valueOf(value);
    }

    @Override
    String formatNonNull(@Nonnull final Double value) {
        return String.valueOf(value);
    }
}
