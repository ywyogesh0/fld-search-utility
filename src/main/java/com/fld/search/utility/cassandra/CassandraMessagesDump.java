package com.fld.search.utility.cassandra;

import com.cedarsoftware.util.io.JsonWriter;
import com.datastax.driver.core.*;
import com.fld.search.utility.app.MessagesDumpApp;
import com.fld.search.utility.util.DateConversionUtility;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static com.fld.search.utility.util.Constants.*;

public class CassandraMessagesDump implements Runnable {

    private Cluster cluster;
    private Session session;
    private String partitionColumnValues;
    private String cassandraKeyspace;
    private String outputFilePath;
    private String outputFilePathDateFormatter;
    private String cassandraHost;
    private String partitionColumnName;
    private Set<String> tableNamesSet = new HashSet<>();
    private List<JSONObject> tableStringJSONList = new ArrayList<>();
    private Set<String> partitionColumnValuesSet = new HashSet<>();

    public CassandraMessagesDump(MessagesDumpApp messagesDumpApp) {
        Properties properties = messagesDumpApp.getProperties();

        this.partitionColumnValues = properties.getProperty(CASSANDRA_PARTITION_COLUMN_VALUES);
        this.cassandraKeyspace = properties.getProperty(CASSANDRA_KEYSPACE);
        this.outputFilePath = properties.getProperty(CASSANDRA_OUTPUT_FILE_PATH);
        this.cassandraHost = properties.getProperty(CASSANDRA_HOST);
        this.partitionColumnName = properties.getProperty(CASSANDRA_PARTITION_COLUMN_NAME);
        this.outputFilePathDateFormatter = properties.getProperty(OUTPUT_FILE_PATH_DATE_FORMATTER);
    }

    @Override
    public void run() {
        try {

            System.out.println("CassandraMessagesDump started...");

            connectCassandraCluster();
            filterTableNames();
            populatePartitionColumnValuesSet();
            prepareTablesDumpData();
            dumpDataInFile();

            System.out.println("CassandraMessagesDump finished successfully...");

        } catch (Exception e) {
            System.out.println("CassandraMessagesDump - ERROR: " + e.getMessage());
        } finally {
            if (session != null) {
                session.close();
            }

            if (cluster != null) {
                cluster.close();
            }
        }
    }

    private void connectCassandraCluster() {
        System.out.println("Connecting Cassandra Cluster on " + cassandraHost + "...");
        cluster = Cluster.builder().addContactPoint(cassandraHost).build();
        session = cluster.connect();
        System.out.println("Cassandra Cluster Connected Successfully...");
    }

    private void filterTableNames() {
        Metadata metadata = cluster.getMetadata();

        Collection<TableMetadata> tablesMetadata = metadata.getKeyspace(cassandraKeyspace).getTables();
        for (TableMetadata tm : tablesMetadata) {

            String tableName = tm.getName();
            Collection<ColumnMetadata> columnsMetadata = tm.getColumns();

            for (ColumnMetadata cm : columnsMetadata) {
                String columnName = cm.getName().toLowerCase();

                if (columnName.equals(partitionColumnName)) {
                    tableNamesSet.add(tableName);
                }
            }
        }
    }

    private void populatePartitionColumnValuesSet() {
        String[] partitionColumnValueArray = partitionColumnValues.split(",");
        if (partitionColumnValueArray.length > 1) {
            Collections.addAll(partitionColumnValuesSet, partitionColumnValueArray);
        } else {
            partitionColumnValuesSet.add(partitionColumnValues);
        }
    }

    private void prepareTablesDumpData() {
        for (String tableName : tableNamesSet) {

            JSONObject tableJSON = new JSONObject();
            List<JSONObject> parentRowJSONList = new ArrayList<>();

            for (String partitionColumnValue : partitionColumnValuesSet) {

                String cqlQuery = "select * from " + cassandraKeyspace + "." + tableName + " where " + partitionColumnName + "=" +
                        Integer.parseInt(partitionColumnValue);

                ResultSet resultSet = session.execute(cqlQuery);
                List<Row> rowList = resultSet.all();

                int rowCount = rowList.size();
                if (rowCount > 0) {
                    ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
                    int columnsCount = columnDefinitions.size();

                    for (Row row : rowList) {
                        JSONObject rowJSON = new JSONObject();
                        for (int j = 0; j < columnsCount; j++) {
                            rowJSON.put(columnDefinitions.getName(j), row.getObject(j));
                        }

                        parentRowJSONList.add(rowJSON);
                    }
                }
            }

            tableJSON.put(tableName, parentRowJSONList);
            tableStringJSONList.add(tableJSON);
        }
    }

    private void dumpDataInFile() {
        String filePath = outputFilePath
                + CassandraMessagesDump.class.getSimpleName() + "-"
                + DateConversionUtility.timestampToFormattedDate(System.currentTimeMillis(), outputFilePathDateFormatter)
                + ".json";

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {
            for (JSONObject tableJSONDump : tableStringJSONList) {
                bufferedWriter.write(JsonWriter.formatJson(tableJSONDump.toString()));
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
