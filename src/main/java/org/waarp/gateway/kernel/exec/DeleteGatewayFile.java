package org.waarp.gateway.kernel.exec;

import java.io.File;

public class DeleteGatewayFile
{
	File filename;
	public void deleteFile(String string) 
	{
		filename=new File(string);
		System.out.println("Gateway file is" +filename);
	}
	public void delete() 
	{
		//filename.delete();
		System.err.println("GatewayFile is deleted");
	}
} 
