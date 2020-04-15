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

import java.util.List;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SheetDataLocation
{
    private final String sheetId;
    private final String tab;
    private final SheetRange range;

    @JsonCreator
    public SheetDataLocation(@JsonProperty("sheetId") String sheetId, @JsonProperty("tab") String tab, @JsonProperty("range") SheetRange range)
    {
        this.sheetId = requireNonNull(sheetId, "sheetId is null");
        this.tab = requireNonNull(tab, "tab is null");
        this.range = requireNonNull(range, "range is null");
    }

    @JsonProperty
    public String getSheetId()
    {
        return sheetId;
    }

    @JsonProperty
    public String getTab()
    {
        return tab;
    }

    @JsonProperty
    public SheetRange getRange()
    {
        return range;
    }

    public String getTabAndRange()
    {
        if (tab.isEmpty()) {
            return range.getRangeExpression();
        }
        else {
            return format("%s!%s", tab, range.getRangeExpression());
        }
    }

    public String getHeaderTabAndRange()
    {
        if (tab.isEmpty()) {
            return range.getHeaderRangeExpression();
        }
        else {
            return format("%s!%s", tab, range.getHeaderRangeExpression());
        }
    }

    public List<SheetDataLocation> partition(int partitionSize)
    {
        return range.partition(partitionSize).stream()
                .map(range -> new SheetDataLocation(sheetId, tab, range))
                .collect(toImmutableList());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SheetDataLocation)) {
            return false;
        }
        SheetDataLocation that = (SheetDataLocation) other;
        return sheetId.equals(that.sheetId) &&
                tab.equals(that.tab) &&
                range.equals(that.range);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sheetId, tab, range);
    }
}
