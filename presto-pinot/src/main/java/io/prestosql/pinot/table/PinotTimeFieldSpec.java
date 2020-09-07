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
package io.prestosql.pinot.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.data.TimeFieldSpec;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotTimeFieldSpec
{
    private final PinotTimeGranularity incomingGranularity;
    private final Optional<PinotTimeGranularity> outgoingGranularity;

    public PinotTimeFieldSpec(TimeFieldSpec timeFieldSpec)
    {
        this(new PinotTimeGranularity(timeFieldSpec.getIncomingGranularitySpec()),
        Optional.of(new PinotTimeGranularity(timeFieldSpec.getOutgoingGranularitySpec())));
    }

    @JsonCreator
    public PinotTimeFieldSpec(
            @JsonProperty PinotTimeGranularity incomingGranularity,
            @JsonProperty Optional<PinotTimeGranularity> outgoingGranularity)
    {
        this.incomingGranularity = requireNonNull(incomingGranularity, "incomingGranularity is null");
        this.outgoingGranularity = requireNonNull(outgoingGranularity, "outgoingGranularity is null");
    }

    @JsonProperty
    public PinotTimeGranularity getIncomingGranularity()
    {
        return incomingGranularity;
    }

    @JsonProperty
    public Optional<PinotTimeGranularity> getOutgoingGranularity()
    {
        return outgoingGranularity;
    }

    public TimeFieldSpec toTimeFieldSpec()
    {
        if (outgoingGranularity.isPresent()) {
            return new TimeFieldSpec(incomingGranularity.toTimeGranularitySpec(),
                    outgoingGranularity.get().toTimeGranularitySpec());
        }
        return new TimeFieldSpec(incomingGranularity.toTimeGranularitySpec());
    }
}
