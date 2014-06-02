package org.sensoriclife.reports.consumptionInDaytime;

import java.io.IOException;
import java.text.DateFormat;
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
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

public class ConsumptionInDaytimeReport {
	public static boolean test = false;

	public static void runReport() throws Exception {

		String[] args = new String[13];
		args[0] = "Report: consumptionInDaytime";
		args[1] = Config.getProperty("accumulo.name");
		args[2] = Config.getProperty("accumulo.zooServers");
		args[3] = Config.getProperty("accumulo.user");
		args[4] = Config.getProperty("accumulo.password");
		args[5] = Config.getProperty("accumulo.consumptionInDaytime.outPutTable.name");
		args[6] = Config.getProperty("accumulo.consumptionInDaytime.outPutTable.zooServers");
		args[7] = Config.getProperty("accumulo.consumptionInDaytime.outPutTable.user");
		args[8] = Config.getProperty("accumulo.consumptionInDaytime.outPutTable.password");
		args[9] = Config.getProperty("accumulo.consumptionInDaytime.dateRange.min");
		args[10] = Config.getProperty("accumulo.consumptionInDaytime.dateRange.max");
		args[11] = Config.getProperty("accumulo.consumptionInDaytime.district").trim();
		args[12] = String.valueOf(new GregorianCalendar().getTimeInMillis());

		if (test) {
			MockInstance inputMockInstance = new MockInstance(args[1]);
			Connector inputConnector = inputMockInstance.getConnector(args[3], new PasswordToken(args[4]));
			inputConnector.tableOperations().create(args[2], false);
			insertData(inputConnector, args[2]);
		}
	
		ConsumptionInDaytimeStarter.runConvert(args);

		Accumulo.getInstance().connect(args[5]);
		Accumulo.getInstance().flushBashWriter(args[6]);
		Accumulo.getInstance().disconnect();

		/*
		 * Test
		 */
		if (test) {
			MockInstance inputMockInstance = new MockInstance(args[1]);
			MockInstance outputMockInstance = new MockInstance(args[5]);

			Connector inputConnector = inputMockInstance.getConnector(args[3], new PasswordToken(args[4]));
			Connector outputConnector = outputMockInstance.getConnector(args[7], new PasswordToken(args[8]));

			printTable(inputConnector, args[2]);
			System.out.println("############################################");
			printTable(outputConnector, args[6]);
		}
	}

	public static void insertData(Connector conn, String tableName) throws IOException, AccumuloException,
			AccumuloSecurityException, TableExistsException, TableNotFoundException, ParseException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter wr = conn.createBatchWriter(tableName, config);

		// ######################################################################
		int consumptionId = 1;
		DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

		Text colFam = new Text("device");
		Text colQual = new Text("amount");
		Text colFam2 = new Text("residential");
		Text colQual2 = new Text("id");
		ColumnVisibility colVis = new ColumnVisibility();// "public");

		Date d = formatter.parse("01.01.2000 00:00:00");
		Mutation mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam2, colQual2, colVis, d.getTime(), new Value("1-2-1-1-1".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam2, colQual2, colVis, d.getTime(), new Value("1-2-2-1-1".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam2, colQual2, colVis, d.getTime(), new Value("1-2-1-1-1".getBytes()));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012 00:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(1))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(1))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(2))));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012 06:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(2))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(2))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(2))));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012 12:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012 18:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(5))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(5))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(5))));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012 00:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(6))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(6))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(6))));
		wr.addMutation(mutation);

		d = formatter.parse("12.03.2012 20:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(7))));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012 04:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(10))));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012 08:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(11))));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012 12:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(12))));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012 16:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(13))));
		wr.addMutation(mutation);

		d = formatter.parse("13.03.2012 20:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(14))));
		wr.addMutation(mutation);

		d = formatter.parse("14.03.2012 00:00:00");
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), new Value(Helpers.toByteArray(new Float(15))));
		wr.addMutation(mutation);

		wr.close();
	}

	public static void printTable(Connector conn, String tableName) throws TableNotFoundException {
		System.out.println("\n Tabelle:" + tableName);
		Scanner scanner = conn.createScanner(tableName, new Authorizations());
		Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Key, Value> entry = iterator.next();
			Key key = entry.getKey();
			Value v = entry.getValue();
			if (key.getColumnQualifier().toString().equals("amount"))// getColumnFamily().toString().equals("device"))
			{
				try {
					System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: "
							+ key.getColumnQualifier() + " Ts:"
							+ new SimpleDateFormat("yyyy-MM-dd HH:mm").format(key.getTimestamp()) + " Value: "
							+ (Float) Helpers.toObject(v.get()));
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: "
						+ key.getColumnQualifier() + " Ts:" + new SimpleDateFormat("yyyy-MM-dd HH:mm").format(key.getTimestamp()) + " Value: " + v);
			}
		}
	}

}
