/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.pinot.decoders;

import io.prestosql.pinot.PinotException;

import java.util.Optional;

import static io.prestosql.pinot.PinotErrorCode.PINOT_DECODE_ERROR;

public abstract class AbstractFixedWidthDecoder
        implements Decoder
{
    protected static final String PINOT_INFINITY = "∞";
    protected static final String PINOT_POSITIVE_INFINITY = "+" + PINOT_INFINITY;
    protected static final String PINOT_NEGATIVE_INFINITY = "-" + PINOT_INFINITY;

    protected static final Double PRESTO_INFINITY = Double.POSITIVE_INFINITY;
    protected static final Double PRESTO_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    protected Double parseDouble(String value)
    {
        try {
            return Double.valueOf(value);
        }
        catch (NumberFormatException ne) {
            switch (value) {
                case PINOT_INFINITY:
                case PINOT_POSITIVE_INFINITY:
                    return PRESTO_INFINITY;
                case PINOT_NEGATIVE_INFINITY:
                    return PRESTO_NEGATIVE_INFINITY;
            }
            throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), "Cannot decode double value from pinot " + value, ne);
        }
    }
}
