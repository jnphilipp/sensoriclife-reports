package org.sensoriclife.util;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;



public class AccumuloDB {

	String instanceName; 
	String zooServers; 
	Instance inst; 
	Connector conn;
	String userName;
	String passwd;
	
	
	public AccumuloDB(String instanceName,String zooServers, String userName, String passwd) {
		super();
		this.instanceName = "myinstance"; // instanceName
		this.zooServers = "zooserver-one,zooserver-two"; // zooServers
		this.userName = userName;
		this.passwd = passwd;
			
	}
	
	public Scanner readDBByKey(String tabelName,String key,String attribute,String authentication)throws AccumuloException, AccumuloSecurityException, TableNotFoundException
	{
		
		this.inst = new ZooKeeperInstance(this.instanceName, this.zooServers);
		this.conn = this.inst.getConnector(userName, new PasswordToken(passwd));
		
		Authorizations auths = new Authorizations(authentication);

		Scanner scan = conn.createScanner(tabelName, auths);
		
		scan.setRange(new Range(key,key));
		
		scan.fetchColumnFamily(new Text(attribute));

		return scan;
	}
	
}
