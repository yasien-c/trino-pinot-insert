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
package io.prestosql.plugin.google.sheets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SheetRange
{
    private static final Pattern RANGE_REGEX = Pattern.compile("\\$(?<begin>\\d+):\\$(?<end>\\d+)");

    private final int begin;
    private final int end;

    public static SheetRange parseRange(String rangeExpression)
    {
        requireNonNull(rangeExpression, "rangeExpression is null");
        Matcher matcher = RANGE_REGEX.matcher(rangeExpression);
        checkState(matcher.matches(), "Malformed range expression '%s'", rangeExpression);
        return new SheetRange(parseInt(matcher.group("begin")), parseInt(matcher.group("end")));
    }

    @JsonCreator
    public SheetRange(@JsonProperty("begin") int begin, @JsonProperty("end") int end)
    {
        checkArgument(begin >= 1, "begin is less than 1");
        checkArgument(end >= 1, "end is less than 1");
        checkState(begin <= end, "begin is greater than end");
        this.begin = begin;
        this.end = end;
    }

    @JsonProperty
    public int getBegin()
    {
        return begin;
    }

    @JsonProperty
    public int getEnd()
    {
        return end;
    }

    public String getRangeExpression()
    {
        return format("$%s:$%s", begin, end);
    }

    public String getHeaderRangeExpression()
    {
        return format("$%1$s:$%1$s", begin);
    }

    public List<SheetRange> partition(int partitionSize)
    {
        ImmutableList.Builder<SheetRange> partitions = ImmutableList.builder();
        int position = begin;
        while (position <= end) {
            partitions.add(new SheetRange(position, min(position + partitionSize - 1, end)));
            position += partitionSize;
        }
        return partitions.build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SheetRange)) {
            return false;
        }
        SheetRange that = (SheetRange) other;
        return begin == that.begin &&
                end == that.end;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(begin, end);
    }

    @Override
    public String toString()
    {
        return getRangeExpression();
    }
}
