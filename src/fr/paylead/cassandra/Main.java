package fr.paylead.cassandra;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Metrics;
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
		options.addOption("s", "stats", false, "Print cassandra stats");
		options.addOption(Option.builder("h").argName("ip").longOpt("host").desc("IP adresse of a node").hasArg(true).build());
		options.addOption(Option.builder("o").argName("fileName").longOpt("out").desc("Generated filename").hasArg(true).build());
		options.addOption(Option.builder("k").argName("keyspaceName").longOpt("keyspace").desc("Keyspace Name to audit").hasArg(true).build());
		options.addOption(Option.builder("p").argName("portNumber").longOpt("port").desc("Port number to connect").hasArg(true).build());
		options.addOption(Option.builder("u").argName("user").longOpt("user").desc("Cassandra connect user").hasArg(true).build());
		options.addOption(Option.builder("P").argName("password").longOpt("pass").desc("Cassandra connect password").hasArg(true).build());
		
		

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
			String file = cmd.getOptionValue("o");
			if (cmd.hasOption("v")) {
				System.out.println("Version " + version);
			} else if (cmd.hasOption("audit")) {
				if (file == null) {
				main.audit(ip, port, login, mdp, keyspace);
				} else {
					main.auditHtml(ip, port, login, mdp, keyspace, file);
				}
			} else if (cmd.hasOption("stats")) {
				main.stat(ip, port, login, mdp, keyspace);
			} else {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("PayLead-Cassandra-Tool", header, options, footer, true);
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
		//System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect(keyspace);
		Metadata meta = dseCluster.getMetadata();
		for (TableMetadata table : meta.getKeyspace(keyspace).getTables()) {
			System.out.println(table.exportAsString());
		}
		dseSession.close();
		dseCluster.close();
	}
	private void auditHtml(String ip, String port, String login, String mdp, String keyspace, String file) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		//System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		StringBuffer stb = new StringBuffer();
		stb.append("<html>\n");
		stb.append("<head>\n");
		stb.append("<style>\n");
		stb.append(".db-table {min-width: 150px; border: 1px solid; float: left; margin: 10px; padding: 0px;} \n");
		stb.append(".cadre {border-bottom: 1px solid;    margin: 0px;  padding: 5px;} \n");
		stb.append(".bottom-line {border-bottom: 1px solid;} \n");
		stb.append("</style>\n");
		stb.append("</head>\n");
		stb.append("<body>\n");
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect(keyspace);
		Metadata meta = dseCluster.getMetadata();
		for (TableMetadata table : meta.getKeyspace(keyspace).getTables()) {
			stb.append("<div class=\"db-table\">\n");
			stb.append("<div class=\"cadre bottom-line\">\n");
			stb.append(table.getName() + "\n");
			stb.append("</div>\n");
			stb.append("<div class=\"cadre bottom-line\">\n");
			for (ColumnMetadata column : table.getColumns()) {
				stb.append("- " + column.getName() + " : " + column.getType() + "<br />\n");
			}
			stb.append("</div>\n");
			stb.append("<div class=\"cadre bottom-line\">\n");
			for (ColumnMetadata column : table.getClusteringColumns()) {
				stb.append("- " + column.getName() + " : " + column.getType() + "<br />\n");
			}
			stb.append("</div>\n");
			stb.append("</div>\n");
		}
		
		stb.append("</body>\n</html>");
		dseSession.close();
		dseCluster.close();
		System.out.println(stb.toString());
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(file));
			 writer.write(stb.toString());		     
			 writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void stat(String ip, String port, String login, String mdp, String keyspace) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		//System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect(keyspace);
		Metrics metrics = dseCluster.getMetrics();
		System.out.println(metrics.getKnownHosts().getValue() + "\t The number of Cassandra hosts currently known by the driver (that is whether they are currently considered up or down)");
		System.out.println(metrics.getConnectedToHosts().getValue() + "\t The number of Cassandra hosts the driver is currently connected to (that is have at least one connection opened to).");
		System.out.println(metrics.getErrorMetrics().getAuthenticationErrors().getCount() + "\t the number of authentication errors while connecting to Cassandra nodes.");
		System.out.println(metrics.getErrorMetrics().getClientTimeouts().getCount() + "\t the number of requests that timed out before the driver received a response.");
		System.out.println(metrics.getErrorMetrics().getConnectionErrors().getCount() + "\t  the number of connection to Cassandra nodes errors.");
		System.out.println(metrics.getErrorMetrics().getIgnores().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, for example due to timeouts or unavailability.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnClientTimeout().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, after a client timeout.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnConnectionError().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, after a connection error.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnOtherErrors().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, after an unexpected error.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnReadTimeout().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, after a read timed out.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnUnavailable().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, after an unavailable exception.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnWriteTimeout().getCount() + "\t the number of times a request was ignored due to the RetryPolicy, after a write timed out.");
		System.out.println(metrics.getErrorMetrics().getOthers().getCount() + "\t the number of requests that returned errors not accounted for by another metric.");
		System.out.println(metrics.getErrorMetrics().getReadTimeouts().getCount() + "\t the number of read requests that returned a timeout (independently of the final decision taken by the RetryPolicy).");
		System.out.println(metrics.getErrorMetrics().getRetries().getCount() + "\t the number of times a request was retried due to the RetryPolicy.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnClientTimeout().getCount() + "\t the number of times a request was retried due to the RetryPolicy, after a client timeout.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnConnectionError().getCount()+ "\t the number of times a request was retried due to the RetryPolicy, after a connection error.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnOtherErrors().getCount() + "\t the number of times a request was retried due to the RetryPolicy, after an unexpected error.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnReadTimeout().getCount() + "\t the number of times a request was retried due to the RetryPolicy, after a read timed out.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnUnavailable().getCount() + "\t the number of times a request was retried due to the RetryPolicy, after an unavailable exception.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnWriteTimeout().getCount() + "\t the number of times a request was retried due to the RetryPolicy, after a write timed out.");
		System.out.println(metrics.getErrorMetrics().getSpeculativeExecutions().getCount() + "\t the number of times a speculative execution was started because a previous execution did not complete within the delay specified by SpeculativeExecutionPolicy.");
		System.out.println(metrics.getErrorMetrics().getUnavailables().getCount() + "\t the number of requests that returned an unavailable exception (independently of the final decision taken by the RetryPolicy).");
		System.out.println(metrics.getErrorMetrics().getWriteTimeouts().getCount() + "\t the number of write requests that returned a timeout (independently of the final decision taken by the RetryPolicy).");
		System.out.println(metrics.getExecutorQueueDepth().getValue() + "\t The number of queued up tasks in the non-blocking executor (Cassandra Java Driver workers).");
		System.out.println(metrics.getOpenConnections().getValue() + "\t  the total number of currently opened connections to Cassandra hosts.");
		System.out.println(metrics.getReconnectionSchedulerQueueSize().getValue() + "\t The size of the work queue for the reconnection scheduler (Reconnection). A queue size > 0 does not necessarily indicate a backlog as some tasks may not have been scheduled to execute yet.");
		
		for(Timer timer : metrics.getRegistry().getTimers().values()) {
			System.out.println("Timer " + timer.getCount()); //TODO ajouter le nom du timer
		}
		System.out.println(metrics.getTaskSchedulerQueueSize().getValue() + "\t The size of the work queue for the task scheduler (Scheduled Tasks). A queue size > 0 does not necessarily indicate a backlog as some tasks may not have been scheduled to execute yet.");
		System.out.println(metrics.getTrashedConnections().getValue() + "\t the total number of currently \"trashed\" connections to Cassandra hosts.");		
		System.out.println(metrics.getBlockingExecutorQueueDepth().getValue() + "\t The number of queued up tasks in the blocking executor (Cassandra Java Driver blocking tasks worker).");
		dseSession.close();
		dseCluster.close();
	}
}
