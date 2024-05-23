package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;

/**
 * @author Liudmila Kornilova
 **/
public class DateToDateCodec implements TypeCodec<Date> {
    private final TypeCodec<LocalDate> dateCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.DATE, LocalDate.class);

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.DATE;
    }

    @NonNull
    @Override
    public GenericType<Date> getJavaType() {
        return GenericType.of(Date.class);
    }

    @Override
    public ByteBuffer encode(Date value, @NonNull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        LocalDate localDate = LocalDate.from(Instant.ofEpochMilli(value.getTime()));
        return dateCodec.encode(localDate, protocolVersion);
    }

    @Override
    public Date decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        LocalDate localDate = dateCodec.decode(bytes, protocolVersion);
        return localDate == null ? null : new Date(localDate.toEpochDay() * 86400L);
    }

    @Override
    public Date parse(String value) {
        throw new RuntimeException("Not supported");
    }

    @Override
    @NonNull
    public String format(Date value) {
        throw new RuntimeException("Not supported");
    }
}
