package fr.paylead.cassandra;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseCluster.Builder;
import com.datastax.driver.dse.DseSession;

public class Main {
	public static String version = "0.1";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Options options = new Options();
		String header = "Add some usefull tools to the amazing Cassandra\n\n";

		options.addOption("help", false, "Print this help");
		options.addOption("v", false, "Version " + version);
		options.addOption("a", "audit", false, "Perform a keyspace audit");
		options.addOption(OptionBuilder.withArgName("ip").withLongOpt("host").withDescription("IP adresse of a node").hasArg(true).create('h'));
		options.addOption(OptionBuilder.withArgName("fileName").withLongOpt("out").withDescription("Generated filename").hasArg(true).create('o'));
		options.addOption(OptionBuilder.withArgName("keyspaceName").withLongOpt("keyspace").withDescription("Keyspace Name to audit").hasArg(true).create('k'));
		options.addOption(OptionBuilder.withArgName("portNumber").withLongOpt("port").withDescription("Port number to connect").hasArg(true).create('p'));
		options.addOption(OptionBuilder.withArgName("user").withLongOpt("user").withDescription("Cassandra connect user").hasArg(true).create('u'));
		options.addOption(OptionBuilder.withArgName("password").withLongOpt("pass").withDescription("Cassandra connect password").hasArg(true).create('P'));
		
			
		String footer = "\nPlease report issues at contact@paylead.fr";
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
				Main main = new Main();
				String ip = cmd.getOptionValue("h");
				String keyspace = cmd.getOptionValue("k");
				String login = cmd.getOptionValue("u");
				String mdp = cmd.getOptionValue("P");
				String port = cmd.getOptionValue("p");
				if (cmd.hasOption("v")) {
					System.out.println("Version " + version);
				} else if (cmd.hasOption("audit")) {
					main.audit(ip, port, login, mdp, keyspace);
				}

				else {

				}
			
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("PayLead-Cassandra-Tool", header, options, footer, true);
		}
	}

	private void audit(String ip, String port, String login, String mdp, String keyspace) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);		
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)).build();		
		dseSession = dseCluster.connect(keyspace);
		Metadata meta = dseCluster.getMetadata();
		for (TableMetadata table : meta.getKeyspace(keyspace).getTables()) {
			System.out.println(table.exportAsString());
		}
	}
}
