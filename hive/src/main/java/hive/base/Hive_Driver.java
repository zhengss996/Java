package hive.base;


import org.apache.hadoop.hive.cli.CliDriver;

public class Hive_Driver {

	public static void main(String[] args) throws Exception{
		System.setProperty("jline.WindowsTerminal.directConsole", "false");
		int ret = new CliDriver().run(args);
		System.exit(ret);
	}

}

