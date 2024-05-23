/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveDoubleCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class FloatToDoubleCodec extends AbstractCodec<Double> implements PrimitiveDoubleCodec {

    @NonNull
    @Override
    public GenericType<Double> getJavaType() {
        return GenericType.DOUBLE;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
        return DataTypes.FLOAT;
    }

    @Override
    public ByteBuffer encodePrimitive(double value, @NonNull ProtocolVersion protocolVersion) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putFloat((float) value);
        bb.flip();
        return bb;
    }

    @Override
    public double decodePrimitive(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() != 4)
            throw new IllegalArgumentException(
                    "Invalid 32-bits float value, expecting 4 bytes but got " + (bytes == null ? null : bytes.remaining()));

        return bytes.getFloat();
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
