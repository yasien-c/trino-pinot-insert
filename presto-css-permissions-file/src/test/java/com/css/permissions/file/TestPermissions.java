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
package com.css.permissions.file;

import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SystemSecurityContext;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.List;

import static org.testng.Assert.assertThrows;

@Test(groups = "test-css")
public class TestPermissions
{
    private static SystemSecurityContext user(String name)
    {
        return new SystemSecurityContext(Identity.ofUser(name));
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }

    @Test
    public void testCatalogRulesEngineering()
    {
        CssDirectorySystemAccessControl accessControl = createAccessControlRules(
                "permissions.json",
                createPrincipal(Lists.newArrayList("Software Engineering")));

        accessControl.checkCanAccessCatalog(user("user"), "hudi_ingest");
        assertDenied(() -> accessControl.checkCanAccessCatalog(user("user"), "recruiting"));
        assertDenied(() -> accessControl.checkCanAccessCatalog(user("user"), "data_science_postgres"));
    }

    @Test
    public void testRulesRecruiting()
    {
        CssDirectorySystemAccessControl accessControl = createAccessControlRules(
                "permissions.json",
                createPrincipal(Lists.newArrayList("Data Science", "Recruiting")));

        accessControl.checkCanAccessCatalog(user("user"), "hudi_ingest");
        accessControl.checkCanAccessCatalog(user("user"), "recruiting");
        assertDenied(() -> accessControl.checkCanAccessCatalog(user("user"), "kafka"));
        accessControl.checkCanSelectFromColumns(user("user"), new CatalogSchemaTableName("hudi_ingest", "recruiting", "table"), null);
        assertDenied(() -> accessControl.checkCanSelectFromColumns(user("user"), new CatalogSchemaTableName("hudi_ingest", "raw", "table"), null));
    }

    @Test
    public void testSchemaRules()
    {
        CssDirectorySystemAccessControl accessControl = createAccessControlRules(
                "permissions.json",
                createPrincipal(Lists.newArrayList("Software Engineering")));

        accessControl.checkCanSelectFromColumns(user("user"), new CatalogSchemaTableName("hudi_ingest", "processed", "table"), null);
        accessControl.checkCanSelectFromColumns(user("user"), new CatalogSchemaTableName("fos_postgres", "test", "table"), null);
        assertDenied(() -> accessControl.checkCanSelectFromColumns(user("user"), new CatalogSchemaTableName("hudi_ingest", "raw", "table"), null));
    }

    private CssDirectorySystemAccessControl createAccessControlRules(String fileName, CssPrincipal principal)
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        CssSystemAccessControlConfig config = new CssSystemAccessControlConfig();
        config.setPermissionsFile(path);
        CssDirectorySystemAccessControl accessControl =
                new CssDirectorySystemAccessControl(config, (accessToken, user) -> principal);
        return accessControl;
    }

    private CssPrincipal createPrincipal(List<String> roles)
    {
        return CssPrincipal.Builder.builder()
                .withRoles(roles)
                .withName("user")
                .withEmail("user@user.com")
                .build();
    }
}
