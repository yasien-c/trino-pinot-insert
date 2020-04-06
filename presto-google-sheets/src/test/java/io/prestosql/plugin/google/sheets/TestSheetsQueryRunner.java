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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.BatchGetValuesByDataFilterRequest;
import com.google.api.services.sheets.v4.model.BatchGetValuesByDataFilterResponse;
import com.google.api.services.sheets.v4.model.DataFilter;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.spi.PrestoException;
import io.prestosql.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.prestosql.plugin.google.sheets.TestSheetsConfig.getProperties;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestSheetsQueryRunner {
    static final String STAGING_METADATA_SHEET_ID = "14Yy_O8OBVLhdv1vA2g7RlvGdv2AIRmrMlxQAO0YdZ30";
    static final String CREDENTIALS_PATH = "/Users/elon/repos/tmp/gcs/gcp-key2.json";
    static final String GOOGLE_SHEETS = "gsheets";

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS_READONLY);
    private static final String APPLICATION_NAME = "presto google sheets integration";

    @Test
    public void testSheetExpression()
    {
        String expression = "1EzLhoNmu7xFBaPAcUPXP3as5FjjJOSnVsRMCGk-DfKU#Sheet!!$1:$2";
        String expression2 = "1EzLhoNmu7xFBaPAcUPXP3as5FjjJOSnVsRMCGk-DfKU#Sheet!";
        String expression3 = "1EzLhoNmu7xFBaPAcUPXP3as5FjjJOSnVsRMCGk-DfKU";
        Pattern pattern = Pattern.compile("(?<sheetId>.*?)(?:#(?<tab>.*?)!(?<range>\\$(?<begin>\\d+):\\$(?<end>\\d+))?)?$");
        Matcher matcher = pattern.matcher(expression);
        if (matcher.matches()) {
            System.out.println(matcher.group("sheetId"));
            System.out.println(matcher.group("tab"));
            System.out.println(matcher.group("range"));
            System.out.println((Integer.parseInt(matcher.group("begin")) + 1));
            System.out.println(matcher.group("end"));
        }
    }

    @Test
    public void testSheetExpression2()
    {
        String expression = "1EzLhoNmu7xFBaPAcUPXP3as5FjjJOSnVsRMCGk-DfKU#Sheet!!$1:$2";
        String expression2 = "1EzLhoNmu7xFBaPAcUPXP3as5FjjJOSnVsRMCGk-DfKU#Sheet!";
        String expression3 = "1EzLhoNmu7xFBaPAcUPXP3as5FjjJOSnVsRMCGk-DfKU";
        Pattern pattern = Pattern.compile("(?<sheetId>.*?)(?:#(?:(?<tab>[^!].*?))?(?:!(?<range>(?<begin>\\$\\d+):\\$\\d+))?)?$");
        Matcher matcher = pattern.matcher(expression3);
        if (matcher.matches()) {
            System.out.println(matcher.group("sheetId"));
            System.out.println(matcher.group("tab"));
            System.out.println(matcher.group("range"));
            System.out.println(matcher.group("begin"));
        }
    }

    @Test
    public void testSheetRegex()
    {
        Pattern pattern = Pattern.compile("(?<sheetId>.*?)(?:#(?<suffix>.+))?$");
        Pattern pattern2 = Pattern.compile("(?<tab>.*?)(?:!?(?<range>(?<begin>\\$\\d+):\\$\\d+))?$");
        String expression = "124fDLUvFrXx2Rql30fbYz8sr2rwgM92RsSVp1AOB2Og#KPI / Goals";
        String expression2 = "124fDLUvFrXx2Rql30fbYz8sr2rwgM92RsSVp1AOB2Og#KPI / Goals!$1:$3";
        Matcher matcher = pattern.matcher(expression);
        if (matcher.matches()) {
            System.out.println(matcher.group("sheetId"));
            System.out.println(matcher.group("suffix"));
            Matcher matcher2 = pattern2.matcher(matcher.group("suffix"));
            if (matcher2.matches()) {

                System.out.println(matcher2.group("range"));
                System.out.println((matcher2.group("begin")));
            }
        }
    }
    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GOOGLE_SHEETS)
                .setSchema("default")
                .build();
    }

    @Test
    public void testService()
            throws Exception
    {
        Sheets sheetsService = new Sheets.Builder(newTrustedTransport(), JacksonFactory.getDefaultInstance(),
                setTimeout(getCredentials(),
                        (int) new Duration(2, TimeUnit.MINUTES).toMillis(),
                        (int) new Duration(10, TimeUnit.SECONDS).toMillis()))
                .setApplicationName(APPLICATION_NAME).build();
        String sheetId = "14quQPFErG-hlpsrNgYcX85vW7JMMK5X2vNZrafRcH8c";
        String tab = "COVID-19 Cases";
        Spreadsheet spreadSheet = sheetsService.spreadsheets().get(sheetId).execute();
        spreadSheet.getSheets();
        BatchGetValuesByDataFilterResponse response = sheetsService.spreadsheets().values().batchGetByDataFilter(sheetId,
                new BatchGetValuesByDataFilterRequest().setDataFilters(
                        ImmutableList.of(
                                new DataFilter().setGridRange(
                                        new GridRange()
                                                .setStartColumnIndex(1)
                                                .setStartRowIndex(1)
                                                .setEndColumnIndex(14)
                                                .setEndRowIndex(spreadSheet.getSheets().get(0).getProperties().getGridProperties().getColumnCount() - 2))))).execute();
/*
        sheetsService.spreadsheets().values().batchGetByDataFilter(sheetId,
                new BatchGetValuesByDataFilterRequest().setDataFilters(
                        ImmutableList.of(
                                new DataFilter().setGridRange(
                                        new GridRange()
                                                .setStartColumnIndex(0)
                                                .setStartRowIndex(0)
                                                .setEndColumnIndex(1000)
                                                .setEndRowIndex(1000))))).execute().getValueRanges();*/
        System.out.println("");
    }

    private static HttpRequestInitializer setTimeout(HttpRequestInitializer initializer, int readTimeout, int connectTimeout)
    {
        return request -> {
            initializer.initialize(request);
            request.setReadTimeout(readTimeout);
            request.setConnectTimeout(connectTimeout);
        };
    }

    private static Credential getCredentials()
    {
        try (InputStream in = new FileInputStream(CREDENTIALS_PATH)) {
            return GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        SheetsPlugin sheetsPlugin = new SheetsPlugin();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setNodeCount(2)
                .setExtraProperties(properties)
                .build();
        queryRunner.installPlugin(sheetsPlugin);
        queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, getProperties(CREDENTIALS_PATH, STAGING_METADATA_SHEET_ID, 1000, "5m"));
        Thread.sleep(10);
        Logger log = Logger.get(TestSheetsQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

    }
}
