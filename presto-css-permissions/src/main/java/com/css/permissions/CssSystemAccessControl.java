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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;

import javax.inject.Inject;

import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.css.permissions.ResourceType.CATALOG;
import static com.css.permissions.ResourceType.COLUMN;
import static com.css.permissions.ResourceType.SCHEMA;
import static com.css.permissions.ResourceType.TABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySelectColumns;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class CssSystemAccessControl
        implements SystemAccessControl
{
    private static final String RESOURCE_SEPARATOR = ":";

    private final AuthorizationClient authorizationClient;

    @Inject
    public CssSystemAccessControl(AuthorizationClient authorizationClient)
    {
        this.authorizationClient = requireNonNull(authorizationClient, "permissionsClient is null");
    }

    /**
     * Check if the principal is allowed to be the specified user.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
    }

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
    }

    /**
     * Check if identity is allowed to access the specified catalog
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalog)
    {
        authorizationClient.validateUser(context);
        if (!authorizationClient.validatePermission(context, Actions.USE, toResource(CATALOG, catalog))) {
            denyCatalogAccess(catalog);
        }
    }

    /**
     * Filter the list of catalogs to those visible to the identity.
     * Although we allow access to the system catalog do not show this as it should be obfuscated and only used by admins.
     * Too much querying will lock important resources.
     */
    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        Set<String> allowedCatalogs = new HashSet<>();
        allowedCatalogs.addAll(ImmutableSet.copyOf(authorizationClient.getAllowedResources(context, Actions.SHOW, toCatalogResources(catalogs))).stream()
                .map(catalog -> extractCatalogName(catalog))
                .collect(toImmutableSet()));
        return allowedCatalogs;
    }

    /**
     * Check if identity is allowed to create the specified schema in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!authorizationClient.validatePermission(context, Actions.CREATE, toResource(SCHEMA, schema.getCatalogName(), schema.getSchemaName()))) {
            denyCreateSchema(schema.toString());
        }
    }

    /**
     * Check if identity is allowed to drop the specified schema in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!authorizationClient.validatePermission(context, Actions.DROP, toResource(SCHEMA, schema.toString()))) {
            denyDropSchema(schema.toString());
        }
    }

    /**
     * Check if identity is allowed to rename the specified schema in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        denyRenameSchema(schema.toString(), newSchemaName);
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
    }

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     */
    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalog, Set<String> schemaNames)
    {
        return authorizationClient.getAllowedResources(context, Actions.SHOW, toSchemaResources(catalog, schemaNames)).stream()
                .map(resource -> extractSchemaName(resource))
                .collect(toImmutableSet());
    }

    /**
     * Check if identity is allowed to create the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.CREATE, toTableResource(table))) {
            denyCreateTable(table.toString());
        }
    }

    /**
     * Check if identity is allowed to drop the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.DROP, toTableResource(table))) {
            denyDropTable(table.toString());
        }
    }

    /**
     * Check if identity is allowed to rename the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        denyRenameTable(table.toString(), newTable.toString());
    }

    /**
     * Check if identity is allowed to comment the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.ALTER, toResource(
                TABLE,
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName()))) {
            denyCommentTable(table.toString());
        }
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowTablesMetadata(SystemSecurityContext context, CatalogSchemaName schema)
    {
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalog, Set<SchemaTableName> tableNames)
    {
        return authorizationClient.getAllowedResources(context, Actions.SHOW, toTableResources(catalog, tableNames.stream()
                .collect(toImmutableSet()))).stream()
                .map(resource -> extractSchemaTableName(resource))
                .collect(toImmutableSet());
    }

    /**
     * Check if identity is allowed to show columns of tables by executing SHOW COLUMNS, DESCRIBE etc.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterColumns} method must filter all results for unauthorized users,
     * since there are multiple ways to list columns.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowColumnsMetadata(SystemSecurityContext context, CatalogSchemaTableName table)
    {
    }

    /**
     * Filter the list of columns to those visible to the identity.
     */
    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns)
    {
        Map<String, ColumnMetadata> columnMap = new HashMap<>();
        columns.stream().forEach(column -> columnMap.put(column.getName(), column));

        Set<String> columnNames = authorizationClient.getAllowedResources(context,
                Actions.SHOW,
                toColumnResources(table, columns.stream()
                        .map(ColumnMetadata::getName)
                        .collect(toImmutableList()))).stream()
                .map(resource -> extractColumnName(table, resource))
                .collect(toImmutableSet());
        return columns.stream()
                .filter(column -> columnNames.contains(column.getName()))
                .collect(toImmutableList());
    }

    /**
     * Check if identity is allowed to add columns to the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.ALTER, toResource(
                TABLE,
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName()))) {
            denyAddColumn(table.toString());
        }
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.ALTER, toResource(
                TABLE,
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName()))) {
            denyDropColumn(table.toString());
        }
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyRenameColumn(table.toString());
    }

    /**
     * Check if identity is allowed to select from the specified columns in a relation.  The column set can be empty.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!authorizationClient.validatePermission(context, Actions.SELECT,
                toResource(
                        TABLE,
                        table.getCatalogName(),
                        table.getSchemaTableName().getSchemaName(),
                        table.getSchemaTableName().getTableName()))) {
            denyQueryTable(table);
        }

        Set<String> allowedResources = authorizationClient.getAllowedResources(context, Actions.SELECT, toColumnResources(table, columns))
                .stream()
                .map(resource -> extractColumnName(table, resource))
                .collect(toImmutableSet());

        Set<String> diff = Sets.difference(columns, allowedResources);
        if (!diff.isEmpty()) {
            denySelectColumns(table.toString(), diff);
        }
    }

    /**
     * Check if identity is allowed to insert into the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.INSERT, toTableResource(table))) {
            denyInsertTable(table.toString());
        }
    }

    /**
     * Check if identity is allowed to delete from the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!authorizationClient.validatePermission(context, Actions.DELETE, toTableResource(table))) {
            denyDeleteTable(table.toString());
        }
    }

    /**
     * Check if identity is allowed to create the specified view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!authorizationClient.validatePermission(context, Actions.CREATE, toTableResource(view))) {
            denyCreateView(view.toString());
        }
    }

    /**
     * Check if identity is allowed to drop the specified view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!authorizationClient.validatePermission(context, Actions.DROP, toTableResource(view))) {
            denyDropView(view.toString());
        }
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified columns in a relation.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        Set<String> allowedResources = authorizationClient.getAllowedResources(context, Actions.SELECT_AS_OWNER, toColumnResources(table, columns))
                .stream()
                .map(resource -> extractColumnName(table, resource))
                .collect(toImmutableSet());

        Set<String> diff = Sets.difference(columns, allowedResources);
        if (!diff.isEmpty()) {
            denyCreateViewWithSelect(table, context.getIdentity(), diff);
        }
    }

    /**
     * Check if identity is allowed to set the specified property in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
    }

    /**
     * Check if identity is allowed to grant the specified privilege to the grantee on the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
        denyGrantTablePrivilege(privilege.name(), table.toString());
    }

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified table from the revokee.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        denyRevokeTablePrivilege(privilege.name(), table.toString());
    }

    /**
     * Check if identity is allowed to show roles on the specified catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName)
    {
    }

    private static String toTableResource(CatalogSchemaTableName table)
    {
        return toResource(TABLE, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName());
    }

    private static Set<String> toCatalogResources(Collection<String> catalogs)
    {
        return catalogs.stream()
                .map(resource -> toResource(CATALOG, resource))
                .collect(toImmutableSet());
    }

    private static Set<String> toSchemaResources(String catalog, Collection<String> schemaNames)
    {
        return schemaNames.stream()
                .map(schema -> toResource(SCHEMA, join(RESOURCE_SEPARATOR, catalog, schema)))
                .collect(toImmutableSet());
    }

    private static Set<String> toTableResources(String catalog, Collection<SchemaTableName> tables)
    {
        return tables.stream()
                .map(schemaTable -> toResource(TABLE, join(RESOURCE_SEPARATOR, catalog, schemaTable.getSchemaName(), schemaTable.getTableName())))
                .collect(toImmutableSet());
    }

    private static Set<String> toColumnResources(CatalogSchemaTableName table, Collection<String> columns)
    {
        return columns.stream()
                .map(column -> toResource(COLUMN, join(RESOURCE_SEPARATOR, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), column)))
                .collect(toImmutableSet());
    }

    private static String toResource(ResourceType resourceType, String... parts)
    {
        checkArgument(parts != null && parts.length > 0, "resource cannot be null or empty");
        StringBuilder builder = new StringBuilder(resourceType.name());

        for (int i = 0; i < parts.length; i++) {
            builder.append(RESOURCE_SEPARATOR)
                    .append(parts[i]);
        }

        return builder.toString();
    }

    private static String extractCatalogName(String resource)
    {
        checkArgument(!isNullOrEmpty(resource), "resource cannot be null or empty");
        List<String> parts = splitToParts(resource, 2);
        checkState(parts.size() >= 2 && ResourceType.valueOf(parts.get(0)) == CATALOG, "malformed schema resource: %s", resource);
        return parts.get(1);
    }

    private static String extractSchemaName(String resource)
    {
        checkArgument(!isNullOrEmpty(resource), "resource cannot be null or empty");
        List<String> parts = splitToParts(resource, 3);
        checkState(parts.size() >= 3 && ResourceType.valueOf(parts.get(0)) == SCHEMA, "malformed schema resource: %s", resource);
        return parts.get(2);
    }

    private static SchemaTableName extractSchemaTableName(String resource)
    {
        checkArgument(!isNullOrEmpty(resource), "resource cannot be null or empty");
        List<String> parts = splitToParts(resource, 4);
        checkState(parts.size() >= 4 && ResourceType.valueOf(parts.get(0)) == TABLE, "malformed table resource: %s", resource);
        return new SchemaTableName(parts.get(2), parts.get(3));
    }

    private static String extractColumnName(CatalogSchemaTableName catalogSchemaTableName, String resource)
    {
        checkArgument(!isNullOrEmpty(resource), "resource cannot be null or empty");
        int prefixLength = COLUMN.name().length()
                + catalogSchemaTableName.getCatalogName().length()
                + catalogSchemaTableName.getSchemaTableName().getSchemaName().length()
                + catalogSchemaTableName.getSchemaTableName().getTableName().length()
                + 4 * RESOURCE_SEPARATOR.length();
        checkState(resource.length() > prefixLength, "malformed column resource: %s", resource);
        return resource.substring(prefixLength);
    }

    private static List<String> splitToParts(String resource, int limit)
    {
        List<String> parts = Splitter.on(RESOURCE_SEPARATOR).limit(limit).splitToList(resource);
        if (parts.size() < 2 || parts.stream().anyMatch(String::isEmpty)) {
            throw new AccessDeniedException(format("Malformed component: %s", parts.get(1)));
        }
        return parts;
    }

    public static void denyQueryTable(CatalogSchemaTableName tableName)
    {
        throw new AccessDeniedException(format("Cannot select from table %s", tableName));
    }

    public static void denyCreateViewWithSelect(CatalogSchemaTableName tableName, Identity identity, Collection<String> columnNames)
    {
        throw new AccessDeniedException(format("View owner '%s' cannot create view that selects %s from %s", identity.getUser(), columnNames, tableName));
    }
}
