package org.sensoriclife.reports.world;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.sensoriclife.world.ResidentialUnit;

public class TestResidentialUnit {
	
	@org.junit.Test
	public void testName() throws Exception {
		
		ResidentialUnit ru = new ResidentialUnit();
		ru.setElectricMeterId("1_el");
		ru.getElecConsumption().setTimestamp(2);
		ru.getElecConsumption().setAmount(3);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bos);
		ru.write(os);
		os.close();
		
		ResidentialUnit ru1 = new ResidentialUnit();
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bis);
		ru1.readFields(ois);
		
		assertEquals("1_el", ru1.getElectricMeterId());
		assertEquals(2, ru1.getElecConsumption().getTimestamp());
		assertEquals(3d, ru1.getElecConsumption().getAmount(), 0d);
	}
}
