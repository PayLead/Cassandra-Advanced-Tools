package fr.paylead.cassandra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Stream;

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
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseCluster.Builder;
import com.datastax.driver.dse.DseSession;

public class Main {
	public static String version = "0.3";

	public static void main(String[] args) {

		Options options = new Options();
		String header = "Add some usefull tools to the amazing Cassandra\n\n";

		options.addOption("help", false, "Print this help");
		options.addOption("v", false, "Version " + version);
		options.addOption("a", "audit", false, "Perform a keyspace audit");
		options.addOption("s", "stats", false, "Print cassandra stats");
		options.addOption(
				Option.builder("h").argName("ip").longOpt("host").desc("IP adresse of a node").hasArg(true).build());
		options.addOption(
				Option.builder("o").argName("fileName").longOpt("out").desc("Generated filename").hasArg(true).build());
		options.addOption(Option.builder("k").argName("keyspaceName").longOpt("keyspace").desc("Keyspace Name to audit")
				.hasArg(true).build());
		options.addOption(Option.builder("p").argName("portNumber").longOpt("port").desc("Port number to connect")
				.hasArg(true).build());
		options.addOption(Option.builder("u").argName("user").longOpt("user").desc("Cassandra connect user")
				.hasArg(true).build());
		options.addOption(Option.builder("P").argName("password").longOpt("pass").desc("Cassandra connect password")
				.hasArg(true).build());
		options.addOption(Option.builder("t").longOpt("tex").desc("Export in latex mode").hasArg(false).build());
		//options.addOption(Option.builder("l").longOpt("load").argName("filePath")
			//	.desc("Create (if not exist) than load the CSV file into table").hasArg(true).build());

		String footer = "\nPlease report issues at contact@klykoo.com";
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
				if (keyspace == null) {
					main.listKeyspaces(ip, port, login, mdp);
				} else if (file == null) {
					main.audit(ip, port, login, mdp, keyspace);
				} else {
					if (cmd.hasOption("t")) {
						main.auditLatex(ip, port, login, mdp, keyspace, file);
					} else {
						main.auditHtml(ip, port, login, mdp, keyspace, file);
					}
				}
			} else if (cmd.hasOption("load")) {
				if (file != null) {
					main.load(ip, port, login, mdp, file);
				}
			} else if (cmd.hasOption("stats")) {
				main.stat(ip, port, login, mdp, keyspace);
			} else {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Cassandra-Tool", header, options, footer, true);
			}

		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Cassandra-Tool", header, options, footer, true);
		}
	}

	private void listKeyspaces(String ip, String port, String login, String mdp) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		// System.out.println("Mdp:" + mdp);

		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect();
		Metadata meta = dseCluster.getMetadata();
		System.out.println("Cluster name: " + meta.getClusterName());
		System.out.println("=======================================");
		for (KeyspaceMetadata keyspace : meta.getKeyspaces()) {
			System.out.println(keyspace.getName());
		}
		dseSession.close();
		dseCluster.close();
	}

	private void load(String ip, String port, String login, String mdp, String filePath) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Laod and create the table.");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		// System.out.println("Mdp:" + mdp);
		System.out.println("File:" + filePath);
		Path path;
		try {
			path = Paths.get(getClass().getClassLoader().getResource(filePath).toURI());

			StringBuilder data = new StringBuilder();
			Stream<String> lines = Files.lines(path);
			lines.forEach(line -> data.append(line).append("\n"));
			lines.close();
		} catch (URISyntaxException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * Builder builder = DseCluster.builder(); builder =
		 * builder.addContactPoint(ip).withCredentials(login, mdp); dseCluster =
		 * builder.withQueryOptions(new
		 * QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
		 * .build(); dseSession = dseCluster.connect(); Metadata meta =
		 * dseCluster.getMetadata(); System.out.println("Cluster name: " +
		 * meta.getClusterName());
		 * System.out.println("======================================="); for
		 * (KeyspaceMetadata keyspace : meta.getKeyspaces()) {
		 * System.out.println(keyspace.getName()); } dseSession.close();
		 * dseCluster.close();
		 */
	}

	private void audit(String ip, String port, String login, String mdp, String keyspace) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		// System.out.println("Mdp:" + mdp);
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
		// System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		StringBuffer stb = new StringBuffer();
		stb.append("<html>\n");
		stb.append("<head>\n");
		stb.append("<style>\n");
		stb.append(".db-table {min-width: 150px; border: 1px solid; float: left; margin: 10px; padding: 0px;} \n");
		stb.append(".cadre {border-bottom: 1px solid;    margin: 0px;  padding: 5px;} \n");
		stb.append(".bottom-line {border-bottom: 1px solid;} \n");
		stb.append(".hide {display: none;} \n");
		stb.append("</style>\n");
		stb.append("<script type=\"text/javascript\">\n");
		stb.append("function myFunction(myDiv) {\n");
		stb.append("var x = document.getElementById(myDiv);\n");
		stb.append("if (x.style.display === \"none\") {\n");
		stb.append("x.style.display = \"block\";\n");
		stb.append(" } else {\n");
		stb.append("x.style.display = \"none\";\n");
		stb.append("}\n");
		stb.append("}\n");
		stb.append("</script>\n");
		stb.append("</head>\n");
		stb.append("<body>\n");
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect(keyspace);
		Metadata meta = dseCluster.getMetadata();
		ArrayList<TableMetadata> list = new ArrayList<TableMetadata>(meta.getKeyspace(keyspace).getTables());
		list.sort((left, right) -> left.getColumns().size() - right.getColumns().size());
		for (TableMetadata table : list) {
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
			for (ColumnMetadata column : table.getPrimaryKey()) {
				stb.append("- " + column.getName() + " : " + column.getType() + "<br />\n");
			}
			stb.append("</div>\n");
			stb.append("<div style=\"cursor: pointer;\" onclick=\"myFunction('myDIV" + table.getName() + "')\">\n");
			stb.append(" More (+)\n");
			stb.append(
					"<div id=\"myDIV" + table.getName() + "\" class=\"cadre bottom-line\" style=\"display: none;\">\n");

			stb.append("- BloomFilterFalsePositiveChance: " + table.getOptions().getBloomFilterFalsePositiveChance()
					+ "<br />\n");
			stb.append("- DefaultTimeToLive: " + table.getOptions().getDefaultTimeToLive() + "<br />\n");
			stb.append("- Comment: " + table.getOptions().getComment() + "<br />\n");
			stb.append("- GcGraceInSeconds: " + table.getOptions().getGcGraceInSeconds() + "<br />\n");
			stb.append("- LocalReadRepairChance: " + table.getOptions().getLocalReadRepairChance() + "<br />\n");
			stb.append("- MemtableFlushPeriodInMs: " + table.getOptions().getMemtableFlushPeriodInMs() + "<br />\n");
			stb.append("- ReadRepairChance: " + table.getOptions().getReadRepairChance() + "<br />\n");
			stb.append("- SpeculativeRetry: " + table.getOptions().getSpeculativeRetry() + "<br />\n");
			stb.append("- CrcCheckChance: " + table.getOptions().getCrcCheckChance() + "<br />\n");
			stb.append("- IndexInterval: " + table.getOptions().getIndexInterval() + "<br />\n");
			stb.append("- MaxIndexInterval: " + table.getOptions().getMaxIndexInterval() + "<br />\n");
			stb.append("- MinIndexInterval: " + table.getOptions().getMinIndexInterval() + "<br />\n");
			stb.append("- Caching: " + table.getOptions().getCaching() + "<br />\n");
			stb.append("- Compaction: " + table.getOptions().getCompaction() + "<br />\n");
			stb.append("- Compression: " + table.getOptions().getCompression() + "<br />\n");
			stb.append("- Extensions: " + table.getOptions().getExtensions() + "<br />\n");
			stb.append("- PopulateIOCacheOnFlush: " + table.getOptions().getPopulateIOCacheOnFlush() + "<br />\n");
			stb.append("- ReplicateOnWrite: " + table.getOptions().getReplicateOnWrite() + "<br />\n");

			stb.append("</div>\n");
			stb.append("</div>\n");
			stb.append("</div>\n");
		}

		stb.append("</body>\n</html>");
		dseSession.close();
		dseCluster.close();
		//System.out.println(stb.toString());
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

	private void auditLatex(String ip, String port, String login, String mdp, String keyspace, String file) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		// System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		StringBuffer stb = new StringBuffer();
		stb.append("\\documentclass[a4paper, 11pt]{article}\n");
		stb.append("\\usepackage[applemac]{inputenc}\n");
		stb.append("\\usepackage[a4paper]{geometry}\n");
		stb.append("\\usepackage[T1]{fontenc}\n");
		stb.append("\\usepackage{lmodern}\n");
		stb.append("\\usepackage{graphicx}\n");
		stb.append("\\usepackage[french]{babel}\n");
		stb.append("\\usepackage{pdflscape}\n");

		stb.append("\\usepackage[inner=0.5in, margin=0in,bottom=0.25in, top=0.25in, outer=3in]{geometry}\n");
		stb.append("\\begin{document}\n");
		// stb.append("\\begin{landscape}\n");
		stb.append("\\setlength{\\fboxrule}{0pt}\n");
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect(keyspace);
		Metadata meta = dseCluster.getMetadata();

		ArrayList<TableMetadata> list = new ArrayList<TableMetadata>(meta.getKeyspace(keyspace).getTables());
		list.sort((left, right) -> left.getColumns().size() - right.getColumns().size());
		for (TableMetadata table : list) {

			stb.append("\\framebox{\n");
			stb.append("\\begin{tabular}{|c|}\n");
			stb.append("\\hline\n");
			stb.append(table.getName().replace("_", "\\_") + " \\\\\n");
			stb.append("\\hline\n");
			for (ColumnMetadata column : table.getColumns()) {
				stb.append("- " + column.getName().replace("_", "\\_") + " : " + column.getType() + " \\\\\n");
			}
			/*
			 * stb.append("</div>\n");
			 * stb.append("<div class=\"cadre bottom-line\">\n"); for
			 * (ColumnMetadata column : table.getPrimaryKey()) { stb.append("- "
			 * + column.getName() + " : " + column.getType() + "<br />\n"); }
			 * stb.append("</div>\n"); stb.
			 * append("<div style=\"cursor: pointer;\" onclick=\"myFunction('myDIV"
			 * +table.getName()+"')\">\n"); stb.append(" More (+)\n");
			 * stb.append("<div id=\"myDIV"+table.getName()
			 * +"\" class=\"cadre bottom-line\" style=\"display: none;\">\n");
			 * 
			 * stb.append("- BloomFilterFalsePositiveChance: " +
			 * table.getOptions().getBloomFilterFalsePositiveChance() +
			 * "<br />\n"); stb.append("- DefaultTimeToLive: " +
			 * table.getOptions().getDefaultTimeToLive() + "<br />\n");
			 * stb.append("- Comment: " + table.getOptions().getComment() +
			 * "<br />\n"); stb.append("- GcGraceInSeconds: " +
			 * table.getOptions().getGcGraceInSeconds() + "<br />\n");
			 * stb.append("- LocalReadRepairChance: " +
			 * table.getOptions().getLocalReadRepairChance() + "<br />\n");
			 * stb.append("- MemtableFlushPeriodInMs: " +
			 * table.getOptions().getMemtableFlushPeriodInMs() + "<br />\n");
			 * stb.append("- ReadRepairChance: " +
			 * table.getOptions().getReadRepairChance() + "<br />\n");
			 * stb.append("- SpeculativeRetry: " +
			 * table.getOptions().getSpeculativeRetry() + "<br />\n");
			 * stb.append("- CrcCheckChance: " +
			 * table.getOptions().getCrcCheckChance() + "<br />\n");
			 * stb.append("- IndexInterval: " +
			 * table.getOptions().getIndexInterval() + "<br />\n");
			 * stb.append("- MaxIndexInterval: " +
			 * table.getOptions().getMaxIndexInterval() + "<br />\n");
			 * stb.append("- MinIndexInterval: " +
			 * table.getOptions().getMinIndexInterval() + "<br />\n");
			 * stb.append("- Caching: " + table.getOptions().getCaching() +
			 * "<br />\n"); stb.append("- Compaction: " +
			 * table.getOptions().getCompaction() + "<br />\n");
			 * stb.append("- Compression: " +
			 * table.getOptions().getCompression() + "<br />\n");
			 * stb.append("- Extensions: " + table.getOptions().getExtensions()
			 * + "<br />\n"); stb.append("- PopulateIOCacheOnFlush: " +
			 * table.getOptions().getPopulateIOCacheOnFlush() + "<br />\n");
			 * stb.append("- ReplicateOnWrite: " +
			 * table.getOptions().getReplicateOnWrite() + "<br />\n");
			 * 
			 * stb.append("</div>\n"); stb.append("</div>\n");
			 * stb.append("</div>\n");
			 */
			stb.append("\\hline\n");
			stb.append("\\end{tabular}\n");
			stb.append("}\n");

		}

		// stb.append("\\end{landscape}\n");
		stb.append("\\end{document}\n");
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
		try {
			System.out.println("latexmk -pdf  -synctex=1 " + file);
			Process p = Runtime.getRuntime().exec("latexmk -pdf -synctex=1 " + file);
			OutputStream out = p.getOutputStream();
			PrintStream prt = new PrintStream(out);
			prt.println();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done");
	}

	private void stat(String ip, String port, String login, String mdp, String keyspace) {
		DseCluster dseCluster = null;
		DseSession dseSession;
		System.out.println("Audit d'une base de données cassandra ");
		System.out.println("Adresse ip:" + ip);
		System.out.println("Port:" + ip);
		System.out.println("Login:" + login);
		// System.out.println("Mdp:" + mdp);
		System.out.println("Keyspace:" + keyspace);
		Builder builder = DseCluster.builder();
		builder = builder.addContactPoint(ip).withCredentials(login, mdp);
		dseCluster = builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
				.build();
		dseSession = dseCluster.connect(keyspace);
		Metrics metrics = dseCluster.getMetrics();
		System.out.println(metrics.getKnownHosts().getValue()
				+ "\t the number of Cassandra hosts currently known by the driver (that is whether they are currently considered up or down)");
		System.out.println(metrics.getConnectedToHosts().getValue()
				+ "\t the number of Cassandra hosts the driver is currently connected to (that is have at least one connection opened to).");
		System.out.println(metrics.getErrorMetrics().getAuthenticationErrors().getCount()
				+ "\t the number of authentication errors while connecting to Cassandra nodes.");
		System.out.println(metrics.getErrorMetrics().getClientTimeouts().getCount()
				+ "\t the number of requests that timed out before the driver received a response.");
		System.out.println(metrics.getErrorMetrics().getConnectionErrors().getCount()
				+ "\t the number of connection to Cassandra nodes errors.");
		System.out.println(metrics.getErrorMetrics().getIgnores().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, for example due to timeouts or unavailability.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnClientTimeout().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, after a client timeout.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnConnectionError().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, after a connection error.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnOtherErrors().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, after an unexpected error.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnReadTimeout().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, after a read timed out.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnUnavailable().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, after an unavailable exception.");
		System.out.println(metrics.getErrorMetrics().getIgnoresOnWriteTimeout().getCount()
				+ "\t the number of times a request was ignored due to the RetryPolicy, after a write timed out.");
		System.out.println(metrics.getErrorMetrics().getOthers().getCount()
				+ "\t the number of requests that returned errors not accounted for by another metric.");
		System.out.println(metrics.getErrorMetrics().getReadTimeouts().getCount()
				+ "\t the number of read requests that returned a timeout (independently of the final decision taken by the RetryPolicy).");
		System.out.println(metrics.getErrorMetrics().getRetries().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnClientTimeout().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy, after a client timeout.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnConnectionError().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy, after a connection error.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnOtherErrors().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy, after an unexpected error.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnReadTimeout().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy, after a read timed out.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnUnavailable().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy, after an unavailable exception.");
		System.out.println(metrics.getErrorMetrics().getRetriesOnWriteTimeout().getCount()
				+ "\t the number of times a request was retried due to the RetryPolicy, after a write timed out.");
		System.out.println(metrics.getErrorMetrics().getSpeculativeExecutions().getCount()
				+ "\t the number of times a speculative execution was started because a previous execution did not complete within the delay specified by SpeculativeExecutionPolicy.");
		System.out.println(metrics.getErrorMetrics().getUnavailables().getCount()
				+ "\t the number of requests that returned an unavailable exception (independently of the final decision taken by the RetryPolicy).");
		System.out.println(metrics.getErrorMetrics().getWriteTimeouts().getCount()
				+ "\t the number of write requests that returned a timeout (independently of the final decision taken by the RetryPolicy).");
		System.out.println(metrics.getExecutorQueueDepth().getValue()
				+ "\t the number of queued up tasks in the non-blocking executor (Cassandra Java Driver workers).");
		System.out.println(metrics.getOpenConnections().getValue()
				+ "\t the total number of currently opened connections to Cassandra hosts.");
		System.out.println(metrics.getReconnectionSchedulerQueueSize().getValue()
				+ "\t the size of the work queue for the reconnection scheduler (Reconnection). A queue size > 0 does not necessarily indicate a backlog as some tasks may not have been scheduled to execute yet.");

		for (Timer timer : metrics.getRegistry().getTimers().values()) {
			System.out.println("Timer " + timer.getCount()); // TODO ajouter le
																// nom du timer
		}
		System.out.println(metrics.getTaskSchedulerQueueSize().getValue()
				+ "\t The size of the work queue for the task scheduler (Scheduled Tasks). A queue size > 0 does not necessarily indicate a backlog as some tasks may not have been scheduled to execute yet.");
		System.out.println(metrics.getTrashedConnections().getValue()
				+ "\t the total number of currently \"trashed\" connections to Cassandra hosts.");
		System.out.println(metrics.getBlockingExecutorQueueDepth().getValue()
				+ "\t The number of queued up tasks in the blocking executor (Cassandra Java Driver blocking tasks worker).");
		dseSession.close();
		dseCluster.close();
	}

	class Sortbyroll implements Comparator<TableMetadata> {
		// Used for sorting in ascending order of
		// roll number
		public int compare(TableMetadata a, TableMetadata b) {
			return a.getColumns().size() - b.getColumns().size();
		}
	}
}
