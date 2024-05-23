package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * @author Liudmila Kornilova
 **/
public class InetToStringCodec implements TypeCodec<String> {
    private final TypeCodec<InetAddress> inetCodec = CodecRegistry.DEFAULT.codecFor(DataTypes.INET, InetAddress.class);

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.INET;
    }

    @NonNull
    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public ByteBuffer encode(String value, @NonNull ProtocolVersion protocolVersion) {
        if (value == null) return null;
        try {
            return inetCodec.encode(InetAddress.getByName(value), protocolVersion);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        if (bytes == null) return null;
        InetAddress address = inetCodec.decode(bytes, protocolVersion);
        return address == null ? null : address.getHostAddress();
    }

    @Override
    public String parse(String value) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    @NonNull
    public String format(String value) {
        throw new RuntimeException("Not implemented");
    }
}
