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
package com.css.permissions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/*
 * Mapping of presto actions to authz actions
 */
enum Actions
{
    USE("READ"),
    SELECT("READ"),
    SELECT_AS_OWNER("READ_AS_OWNER"),
    INSERT("MODIFY"),
    DELETE("MODIFY"),
    SHOW("READ"),
    ALTER("ALTER"),
    CREATE("CREATE"),
    DROP("DROP");

    private final String action;

    Actions(String action)
    {
        checkArgument(!isNullOrEmpty(action), "action cannot be null or empty");
        this.action = action;
    }

    public String getAction()
    {
        return action;
    }
}
