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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;

import javax.inject.Inject;

import java.nio.file.Paths;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.css.permissions.file.AccessMode.ALL;
import static com.css.permissions.file.AccessMode.READ_ONLY;
import static com.css.permissions.file.Constants.ACCESS_TOKEN;
import static com.css.permissions.file.Utils.parseJson;
import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateViewWithSelect;
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
import static io.prestosql.spi.security.AccessDeniedException.denySelectTable;
import static java.util.Objects.requireNonNull;

public class CssDirectorySystemAccessControl
        implements SystemAccessControl
{
    private static final String CSS_VIEW_OWNER = "";
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final Logger log = Logger.get(CssDirectorySystemAccessControl.class);

    private final List<CatalogAccessControlRule> catalogRules;
    private final Map<String, List<SchemaAccessControlRule>> schemaRules;
    private final CssDirectoryClient cssDirectoryClient;

    @Inject
    public CssDirectorySystemAccessControl(
            CssSystemAccessControlConfig config, CssDirectoryClient cssDirectoryClient)
    {
        requireNonNull(config);
        this.cssDirectoryClient = requireNonNull(cssDirectoryClient);

        FileBasedSystemAccessControlRules rules =
                parseJson(Paths.get(config.getPermissionsFile()), FileBasedSystemAccessControlRules.class);

        catalogRules = buildCatalogRules(rules);
        schemaRules = buildSchemaRules(rules);
    }

    private Map<String, List<SchemaAccessControlRule>> buildSchemaRules(
            FileBasedSystemAccessControlRules fileRules)
    {
        ImmutableMap.Builder<String, List<SchemaAccessControlRule>> schemaRulesBuilder =
                ImmutableMap.builder();

        for (SchemaAccessControlRules schemaAccessControlRules : fileRules.getSchemaRules()) {
            String catalog = schemaAccessControlRules.getCatalog();
            ImmutableList.Builder<SchemaAccessControlRule> rulesBuilder = ImmutableList.builder();

            // Making sure users can access informational schema if they can access catalog.
            rulesBuilder.add(
                    new SchemaAccessControlRule(
                            READ_ONLY,
                            Optional.of(Pattern.compile(".*")),
                            Optional.of(Pattern.compile(INFORMATION_SCHEMA))));
            for (SchemaAccessControlRule accessControlRule : schemaAccessControlRules.getRules()) {
                rulesBuilder.add(accessControlRule);
            }

            schemaRulesBuilder.put(catalog, rulesBuilder.build());
        }

        return schemaRulesBuilder.build();
    }

    private List<CatalogAccessControlRule> buildCatalogRules(
            FileBasedSystemAccessControlRules fileRules)
    {
        ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
        catalogRulesBuilder.addAll(fileRules.getCatalogRules());

        // Hack to allow Presto Admin to access the "system" catalog for retrieving server status.
        // todo Change userRegex from ".*" to one particular user that Presto Admin will be restricted
        // to run as
        catalogRulesBuilder.add(
                new CatalogAccessControlRule(
                        ALL, Optional.of(Pattern.compile(".*")), Optional.of(Pattern.compile("system"))));

        return catalogRulesBuilder.build();
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {}

    @Override
    public void checkCanSetSystemSessionProperty(
            SystemSecurityContext context, String propertyName)
    {}

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!canAccessCatalog(context.getIdentity(), catalogName, READ_ONLY, true)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(context.getIdentity(), catalog, READ_ONLY)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    private boolean canAccessCatalog(
            Identity identity, String catalogName, AccessMode requiredAccess, boolean selectAsOwner)
    {
        log.debug(
                "Checking access with selectAsOwner for user {%s} catalog {%s} and access mode {%s}",
                identity.getUser(), catalogName, requiredAccess.name());
        if (identity.getUser().equals(CSS_VIEW_OWNER) && selectAsOwner) {
            return true;
        }

        return canAccessCatalog(identity, catalogName, requiredAccess);
    }

    private boolean canAccessCatalog(
            Identity identity, String catalogName, AccessMode requiredAccess)
    {
        List<String> rolesToCheck = getRolesForCurrentUser(identity);
        log.debug(
                "Checking access for catalog {%s}, user {%s} and mode {%s}",
                catalogName, identity.getUser(), requiredAccess.name());

        // We will check every role user has an will pick the one with highest privileges.
        boolean canAccessCatalog = false;
        for (CatalogAccessControlRule rule : catalogRules) {
            for (String userRole : rolesToCheck) {
                Optional<AccessMode> accessMode = rule.match(userRole, catalogName);
                if (accessMode.isPresent()) {
                    boolean canRoleAccess = accessMode.get().implies(requiredAccess);
                    log.debug(
                            "User {%s} role {%s} has {%s} access mode for catalog {%s}. Requested mode {%s}, access is {%s}",
                            identity.getUser(),
                            userRole,
                            accessMode.get(),
                            catalogName,
                            requiredAccess.name(),
                            canRoleAccess);

                    canAccessCatalog |= canRoleAccess;
                    if (canAccessCatalog) {
                        break;
                    }
                }
            }
        }

        log.info(
                "User {%s} {%s} access to catalog {%s}",
                identity.getUser(), (canAccessCatalog) ? "granted" : "denied", catalogName);
        return canAccessCatalog;
    }

    private boolean canAccessSchema(
            Identity identity,
            String catalogName,
            String schemaName,
            AccessMode requiredAccess,
            boolean selectAsOwner)
    {
        log.debug(
                "Checking access with selectAsOwner for user {%s} catalog {%s} and access mode {%s}",
                identity.getUser(), catalogName, requiredAccess.name());
        if (identity.getUser().equals(CSS_VIEW_OWNER) && selectAsOwner) {
            return true;
        }

        return canAccessSchema(identity, catalogName, schemaName, requiredAccess);
    }

    private boolean canAccessSchema(
            Identity identity, String catalogName, String schemaName, AccessMode requiredAccess)
    {
        log.debug(
                "Checking schema access for user {%s} catalog {%s} schema {%s} and access mode {%s}",
                identity.getUser(), catalogName, schemaName, requiredAccess.name());

        List<String> rolesToCheck = getRolesForCurrentUser(identity);

        // We will check every role user has an will pick the one with highest privileges.
        boolean canAccessSchema = false;
        List<SchemaAccessControlRule> rules = schemaRules.getOrDefault(catalogName, new ArrayList<>());
        if (rules.isEmpty()) {
            log.debug("Schema rules for catalog {%s} are not specified. Allowing access", catalogName);
            canAccessSchema = true;
        }
        else {
            for (SchemaAccessControlRule rule : rules) {
                for (String userRole : rolesToCheck) {
                    Optional<AccessMode> accessMode = rule.match(userRole, schemaName);
                    if (accessMode.isPresent()) {
                        boolean canRoleAccess = accessMode.get().implies(requiredAccess);
                        log.debug(
                                "User {%s} role {%s} has {%s} access mode for schema {%s} in catalog {%s}. Requested mode {%s}, access is {%s}",
                                identity.getUser(),
                                userRole,
                                accessMode.get(),
                                schemaName,
                                catalogName,
                                requiredAccess.name(),
                                canRoleAccess);

                        canAccessSchema |= canRoleAccess;
                        if (canAccessSchema) {
                            break;
                        }
                    }
                }
            }
        }

        log.info(
                "User {%s} {%s} access to catalog {%s} schema {%s}",
                identity.getUser(), (canAccessSchema) ? "granted" : "denied", catalogName, schemaName);
        return canAccessSchema;
    }

    private List<String> getRolesForCurrentUser(Identity identity)
    {
        List<String> rolesToCheck = new ArrayList<>();
        if (!identity.getPrincipal().isPresent()) {
            CssPrincipal principal =
                    cssDirectoryClient.getUserPrincipalAndValidateUser(
                            identity.getExtraCredentials().get(ACCESS_TOKEN), identity.getUser());

            rolesToCheck.addAll(principal.getRoles());
        }
        else {
            Principal principal = identity.getPrincipal().get();
            if (principal instanceof CssPrincipal) {
                rolesToCheck.addAll(((CssPrincipal) principal).getRoles());
            }
            else {
                throw new CssFileAuthenticationException(
                        String.format(
                                "Principal is not CssPrincipal. Type is: {%s}", principal.getClass().getName()));
            }
        }
        return rolesToCheck;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denyCreateSchema(schema.toString());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denyDropSchema(schema.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(), schema.getCatalogName(), schema.getSchemaName(), ALL)) {
            denyDropSchema(schema.toString());
        }
    }

    @Override
    public void checkCanRenameSchema(
            SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }

        if (!canAccessSchema(
                context.getIdentity(), schema.getCatalogName(), schema.getSchemaName(), ALL)) {
            denyDropSchema(schema.toString());
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {}

    @Override
    public Set<String> filterSchemas(
            SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(context.getIdentity(), catalogName, READ_ONLY)) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<String> filteredSchemas = ImmutableSet.builder();
        for (String schema : schemaNames) {
            if (canAccessSchema(context.getIdentity(), catalogName, schema, READ_ONLY)) {
                filteredSchemas.add(schema);
            }
        }
        return filteredSchemas.build();
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyCreateTable(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyDropTable(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(
            SystemSecurityContext context,
            CatalogSchemaTableName table,
            CatalogSchemaTableName newTable)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyRenameTable(table.toString(), newTable.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyCommentTable(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(SystemSecurityContext context, CatalogSchemaName schema) {}

    @Override
    public Set<SchemaTableName> filterTables(
            SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(context.getIdentity(), catalogName, READ_ONLY)) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<SchemaTableName> filteredTables = ImmutableSet.builder();
        for (SchemaTableName table : tableNames) {
            if (canAccessSchema(context.getIdentity(), catalogName, table.getSchemaName(), READ_ONLY)) {
                filteredTables.add(table);
            }
        }
        return filteredTables.build();
    }

    @Override
    public void checkCanShowColumnsMetadata(
            SystemSecurityContext context, CatalogSchemaTableName table)
    {}

    @Override
    public List<ColumnMetadata> filterColumns(
            SystemSecurityContext context,
            CatalogSchemaTableName tableName,
            List<ColumnMetadata> columns)
    {
        if (!canAccessCatalog(context.getIdentity(), tableName.getCatalogName(), READ_ONLY)) {
            return ImmutableList.of();
        }

        if (!canAccessSchema(
                context.getIdentity(),
                tableName.getCatalogName(),
                tableName.getSchemaTableName().getSchemaName(),
                READ_ONLY)) {
            return ImmutableList.of();
        }

        return columns;
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyAddColumn(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyDropColumn(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropColumn(table.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyRenameColumn(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(
            SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), READ_ONLY)) {
            denySelectTable(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                READ_ONLY)) {
            denySelectTable(table.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyInsertTable(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyDeleteTable(table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(context.getIdentity(), view.getCatalogName(), ALL)) {
            denyCreateView(view.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                view.getCatalogName(),
                view.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDeleteTable(view.toString());
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(context.getIdentity(), view.getCatalogName(), ALL)) {
            denyDropView(view.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                view.getCatalogName(),
                view.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(
            SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL, true)) {
            denyCreateViewWithSelect(table.toString(), context.getIdentity());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL,
                true)) {
            denyCreateViewWithSelect(table.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(
            SystemSecurityContext context, String catalogName, String propertyName)
    {}

    @Override
    public void checkCanGrantTablePrivilege(
            SystemSecurityContext context,
            Privilege privilege,
            CatalogSchemaTableName table,
            PrestoPrincipal grantee,
            boolean withGrantOption)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyGrantTablePrivilege(privilege.toString(), table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropView(table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(
            SystemSecurityContext context,
            Privilege privilege,
            CatalogSchemaTableName table,
            PrestoPrincipal revokee,
            boolean grantOptionFor)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }

        if (!canAccessSchema(
                context.getIdentity(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                ALL)) {
            denyDropView(table.toString());
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {}
}
