package org.sensoriclife.reports.emptyResidentialUnitsConsumption;

import java.io.IOException;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.helper.ConsumptionGeneralizeReport;
import org.sensoriclife.util.Helpers;

public class EmptyResidentialUnitsConsumptionReport {

	public static boolean test = true;

	public static boolean runEmptyResidentialUnitsConsumption()
			throws Exception {

		Map<String, String> confs = Config.toMap();

		GregorianCalendar now = new GregorianCalendar();
		long reportTimestamp = now.getTimeInMillis();

		String instanceName = "";
		String zooServer = "";
		String userName = "";
		String password = "";
		String outputTableName = "";
		String inputTableName = "";

		if (confs.containsKey("accumulo.name")) {
			instanceName = confs.get("accumulo.name");
		} else {
			Logger.error("ConfigError: accumulo.name doesn't exist");
			return false;
		}
		if (confs.containsKey("accumulo.zooServers")) {
			zooServer = confs.get("accumulo.zooServers");
		} else {
			Logger.error("ConfigError: accumulo.zooServers doesn't exist");
			return false;
		}
		if (confs.containsKey("accumulo.user")) {
			userName = confs.get("accumulo.user");
		} else {
			Logger.error("ConfigError: accumulo.user doesn't exist");
			return false;
		}
		if (confs.containsKey("accumulo.password")) {
			password = confs.get("accumulo.password");
		} else {
			Logger.error("ConfigError: accumulo.password doesn't exist");
			return false;
		}
		if (confs
				.containsKey("reports.emptyResidentialUnitsConsumption.outputTable.tableName")) {
			outputTableName = confs
					.get("reports.emptyResidentialUnitsConsumption.outputTable.tableName");
		} else {
			Logger.error("ConfigError: reports.emptyResidentialUnitsConsumption.outputTable.tableName doesn't exist");
			return false;
		}
		if (confs.containsKey("accumulo.tableName")) {
			inputTableName = confs.get("accumulo.tableName");
		} else {
			Logger.error("ConfigError: accumulo.tableName doesn't exist");
			return false;
		}

		if (test) {

			MockInstance inputMockInstance = new MockInstance(instanceName);
			MockInstance outputMockInstance = new MockInstance(instanceName);

			Connector inputConnector = inputMockInstance.getConnector("",
					new PasswordToken(""));
			Connector outputConnector = outputMockInstance.getConnector("",
					new PasswordToken(""));

			inputConnector.tableOperations().create(inputTableName, false);
			outputConnector.tableOperations().create(outputTableName, false);

			// insert some test data
			insertData(inputConnector, inputTableName);

		}
		if (test) {
			Accumulo.getInstance().connect(instanceName);
		} else {
			Accumulo.getInstance().connect(instanceName, zooServer, userName,
					password);
			Accumulo.getInstance().createTable(outputTableName);
		}
		Accumulo.getInstance().addMutation(outputTableName,
				Helpers.toByteArray("YearInvoice"),
				Helpers.toByteArray("report"), Helpers.toByteArray("version"),
				Helpers.toByteArray(reportTimestamp));
		Accumulo.getInstance().flushBashWriter(outputTableName);
		Accumulo.getInstance().disconnect();

		String[] jobArgs = new String[5];
		jobArgs[0] = outputTableName;
		
		if(confs.containsKey("reports.emptyResidentialUnitsConsumption.dateRange.min")){
			jobArgs[1] = confs.get("reports.emptyResidentialUnitsConsumption.dateRange.min");}
		else{jobArgs[1] = "";}
		if(confs.containsKey("reports.emptyResidentialUnitsConsumption.dateRange.max")){
			jobArgs[2] = confs.get("reports.emptyResidentialUnitsConsumption.dateRange.max");}
		else{jobArgs[2] = "";}
		if(confs.containsKey("reports.emptyResidentialUnitsConsumption.dataRange.onlyInTimeRange")){
			jobArgs[3] = confs.get("reports.emptyResidentialUnitsConsumption.dataRange.onlyInTimeRange");}
		else{jobArgs[3] = "false";}
		
		jobArgs[4] = new Long(reportTimestamp).toString();
		// first report: merge residentialID,consumptionID and Consumption ->
		// filter timestamp
		ConvertMinMaxTimeStampWithoutUserReport.test = test;
		ConvertMinMaxTimeStampWithoutUserReport.runConvert(jobArgs);

		// second report: consumption for a residentialUnits
		jobArgs = new String[3];
		jobArgs[0] = outputTableName;
		jobArgs[1] = "6";
		jobArgs[2] = new Long(reportTimestamp).toString();
		ConsumptionGeneralizeReport.test = test;
		ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);

		/*
		 * Test
		 */
		if (test) {
			MockInstance inputMockInstance = new MockInstance(instanceName);
			MockInstance outputMockInstance = new MockInstance(instanceName);

			Connector inputConnector = inputMockInstance.getConnector("",
					new PasswordToken(""));
			Connector outputConnector = outputMockInstance.getConnector("",
					new PasswordToken(""));

			// print the inputtable
			printTable(inputConnector, inputTableName);
			System.out.println("############################################");
			// print the results of mapreduce
			printTable(outputConnector, outputTableName);
		}

		return true;
	}

	public static void insertData(Connector conn, String tableName)
			throws IOException, AccumuloException, AccumuloSecurityException,
			TableExistsException, TableNotFoundException, ParseException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter wr = conn.createBatchWriter(tableName, config);

		// ######################################################################
		int consumptionId = 1;
		DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy");

		byte[] colFam = Helpers.toByteArray("device");
		byte[] colQual = Helpers.toByteArray("amount");
		byte[] colFam2 = Helpers.toByteArray("residential");
		byte[] colQual2 = Helpers.toByteArray("id");
		ColumnVisibility colVis = new ColumnVisibility();// "public");
		// long timestamp = System.currentTimeMillis();

		Float floatValue = new Float(1);
		Date d = formatter.parse("12.03.2012");
		// mutation with consumptionId as rowId
		Mutation mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(floatValue));
		mutation.put(colFam2, colQual2, colVis, 2,
				Helpers.toByteArray("1-1-1-1-1"));
		mutation.put(Helpers.toByteArray("user"), Helpers.toByteArray("id"),
				colVis, 2, Helpers.toByteArray("Ich Bin ein User"));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(2)));
		wr.addMutation(mutation);

		d = formatter.parse("14.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(3)));
		wr.addMutation(mutation);

		d = formatter.parse("13.04.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(4)));
		wr.addMutation(mutation);

		d = formatter.parse("13.10.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),
				Helpers.toByteArray(new Float(6)));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),
				Helpers.toByteArray(new Float(7)));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				Helpers.toByteArray("1-1-1-1-2"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(1)));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(2)));
		wr.addMutation(mutation);

		d = formatter.parse("14.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(3)));
		wr.addMutation(mutation);

		d = formatter.parse("13.04.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(4)));
		wr.addMutation(mutation);

		d = formatter.parse("13.10.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(10)));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),
				Helpers.toByteArray(new Float(13)));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),
				Helpers.toByteArray(new Float(15)));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(5)));
		mutation.put(colFam2, colQual2, colVis, 2,
				Helpers.toByteArray("1-1-1-1-1"));
		wr.addMutation(mutation);

		d = formatter.parse("14.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		d = formatter.parse("13.04.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		d = formatter.parse("13.10.2012");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String
				.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),
				Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);

		wr.close();
	}

	public static void printTable(Connector conn, String tableName)
			throws TableNotFoundException {

		System.out.println("\n Tabelle:" + tableName);
		Scanner scanner = conn.createScanner(tableName, new Authorizations());
		Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Key, Value> entry = iterator.next();
			Key key = entry.getKey();
			Value v = entry.getValue();
			Date date = new Date(key.getTimestamp());
			Format format = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

			String timestamp = format.format(date).toString();

			if (key.getColumnQualifier().toString().equals("amount"))// getColumnFamily().toString().equals("device"))
			{
				try {

					System.out.println("Row: "
							+ (String) Helpers
									.toObject(key.getRow().getBytes())
							+ " Fam: "
							+ (String) Helpers.toObject(key.getColumnFamily()
									.getBytes())
							+ " Qual: "
							+ (String) Helpers.toObject(key
									.getColumnQualifier().getBytes()) + " Ts:"
							+ timestamp + " Value: "
							+ (Float) Helpers.toObject(v.get()));
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					System.out.println("Row: "
							+ (String) Helpers
									.toObject(key.getRow().getBytes())
							+ " Fam: "
							+ (String) Helpers.toObject(key.getColumnFamily()
									.getBytes())
							+ " Qual: "
							+ (String) Helpers.toObject(key
									.getColumnQualifier().getBytes()) + " Ts:"
							+ timestamp + " Value: "
							+ Helpers.toObject(v.get()));
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
