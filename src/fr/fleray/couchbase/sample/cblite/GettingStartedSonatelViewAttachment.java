package fr.fleray.couchbase.sample.cblite;

import java.awt.Dimension;
import java.awt.Image;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.imageio.ImageIO;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.couchbase.lite.BasicAuthenticator;
import com.couchbase.lite.Blob;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DataSource;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.Dictionary;
import com.couchbase.lite.Document;
import com.couchbase.lite.DocumentFlag;
import com.couchbase.lite.DocumentReplication;
import com.couchbase.lite.DocumentReplicationListener;
import com.couchbase.lite.Endpoint;
import com.couchbase.lite.Expression;
import com.couchbase.lite.Function;
import com.couchbase.lite.IndexBuilder;
import com.couchbase.lite.MaintenanceType;
import com.couchbase.lite.Meta;
import com.couchbase.lite.MutableDocument;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryBuilder;
import com.couchbase.lite.ReplicatedDocument;
import com.couchbase.lite.Replicator;
import com.couchbase.lite.ReplicatorConfiguration;
import com.couchbase.lite.Result;
import com.couchbase.lite.ResultSet;
import com.couchbase.lite.SelectResult;
import com.couchbase.lite.URLEndpoint;
import com.couchbase.lite.ValueIndexItem;

public class GettingStartedSonatelViewAttachment {

	private static boolean writeNewDocument = false;
	private static final String DB_NAME = "sonatel";
	/*
	 * Credentials declared this way purely for expediency in this demo - use OAUTH
	 * in production code
	 */
	private static final String DB_USER = "marius"; // (Bretagne) OR wolfgang (Alsace) marius (PACA)
	private static final String DB_PASS = "password";

	private static final String SYNC_GATEWAY_URL = "ws://localhost:4984/sonatel"; // "ws://52.174.108.107:4984/stime";
	private static final String DB_PATH = new File("").getAbsolutePath() + "/resources";

	private static Random RANDOM = new Random();

	private static String Prop_Id = "id";
	private static String Prop_Name = "name";
	private static String Prop_TimeStamp = "ts";
	private static String Prop_Status = "status";
	private static String Prop_AvgDev = "avg_deviation";

	private static String Prop_Type = "type";
	private static String searchStringType = "upgrade"; // TODO: to change !!!

	private static String Prop_Channels = "channels";

	private static JComboBox<Map<String, Blob>> cb = new JComboBox<Map<String, Blob>>();
	private static JLabel picLabel = null;
	private static JPanel panel = new JPanel();

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		cb.addItemListener(new ItemListener() {

			@Override
			public void itemStateChanged(ItemEvent e) {
				Object source = e.getSource();
				if (source instanceof JComboBox) {
					JComboBox cb = (JComboBox) source;
					Map<String, Blob> selectedItem = (Map<String, Blob>) cb.getSelectedItem();

					Blob blob = (Blob) selectedItem.values().toArray()[0];
					
					if(null != picLabel) {
						panel.remove(picLabel);
					}
					ImageIcon imageIcon = new ImageIcon(blob.getContent());
					Image image = imageIcon.getImage(); // transform it 
					Image newimg = image.getScaledInstance(500, 300,  java.awt.Image.SCALE_SMOOTH); // scale it the smooth way  
					imageIcon = new ImageIcon(newimg);  // transform it back
					
					picLabel = new JLabel(imageIcon);
					picLabel.setMinimumSize(new Dimension(500, 300));
					panel.add(picLabel);
					panel.revalidate();
					panel.repaint();
				}
			}
		});

		try {

			// Schedule a job for the event-dispatching thread:
			// creating and showing this application's GUI.
			javax.swing.SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					createAndShowGUI();
				}
			});

			CouchbaseLite.init();

			// Get the database (and create it if it doesn’t exist).
			DatabaseConfiguration config = new DatabaseConfiguration();
//			config.setEncryptionKey(encryptionKey)
			config.setDirectory(DB_PATH);
			// config.setEncryptionKey(new EncryptionKey(DB_PASS));
			Database database = new Database(DB_NAME, config);

			List<String> indexes = database.getIndexes();

			if (!indexes.contains("Prop_Type_Index")) {
				database.createIndex("Prop_Type_Index", IndexBuilder.valueIndex(ValueIndexItem.property("type")));
			}

			System.out.println("List of indexes:");
			for (String ind : indexes) {
				System.out.println(ind);
			}

			// Create a query to fetch documents of type "product".
			System.out.println("== Executing Query 1");
			Query query = QueryBuilder.select(SelectResult.all()).from(DataSource.database(database))
					.where(Expression.property(Prop_Type).equalTo(Expression.string(searchStringType)));
			ResultSet result = query.execute();
			System.out.println(
					String.format("Query returned %d rows of type %s", result.allResults().size(), searchStringType));

			Expression COUNT = Function.max(Meta.sequence);
			Query maxSequenceQuery = QueryBuilder.select(SelectResult.expression(COUNT))
					.from(DataSource.database(database))
					.where(Expression.property(Prop_Type).equalTo(Expression.string(searchStringType))); // your
																											// condition
																											// (predicate)

			ResultSet execute = maxSequenceQuery.execute();

			List<Result> allResults = execute.allResults();

			Result result2 = allResults.get(0);
			long long1 = result2.getLong(0);

			Endpoint targetEndpoint = new URLEndpoint(new URI(SYNC_GATEWAY_URL));
			ReplicatorConfiguration replConfig = new ReplicatorConfiguration(database, targetEndpoint);
			replConfig.setReplicatorType(ReplicatorConfiguration.ReplicatorType.PUSH_AND_PULL);

			replConfig.setContinuous(true);

			// Add authentication.
			replConfig.setAuthenticator(new BasicAuthenticator(DB_USER, DB_PASS.toCharArray()));

			// Create replicator (be sure to hold a reference somewhere that will prevent
			// the Replicator from being GCed)
			Replicator replicator = new Replicator(replConfig);

			// Listen to replicator change events.
			replicator.addChangeListener(change -> {
				if (change.getStatus().getError() != null) {
					System.err.println("Error code ::  " + change.getStatus().getError().getCode());
					System.err.println("XXXXXXXXXX ::  " + new Date());
				}
			});

			replicator.addDocumentReplicationListener(new DocumentReplicationListener() {

				@Override
				public void replication(DocumentReplication documentReplication) {
					for (ReplicatedDocument rep : documentReplication.getDocuments()) {
						System.err.println("Document " + rep.getID() + " has been replicated !!");

						// check if document has an attachment
						Document document = database.getDocument(rep.getID());
						List<String> keys = document.getKeys();
						Dictionary dictionary = document.getDictionary("_attachments");
						for (String key : dictionary.getKeys()) {

							Blob blob = dictionary.getBlob(key);
							Map<String, Blob> map = new HashMap<String, Blob>();
							map.put(key, blob);
							cb.addItem(map);
						}

						if (rep.getFlags().contains(DocumentFlag.DELETED)) {
							System.err.println("Document " + rep.getID() + " has been DELETED !!");
							System.err.println("Local purge of the document " + rep.getID() + "... ");
							try {
								database.purge(rep.getID());
							} catch (CouchbaseLiteException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							System.err.println("Local purge of the document " + rep.getID() + "... DONE !");
						} else if (rep.getFlags().contains(DocumentFlag.ACCESS_REMOVED)) {
							System.err.println("Document " + rep.getID()
									+ " ACCESS has been removed => probably a channel change for this document !!");

							// Decide what to do : here I decide to purge the document since
							// I should not get access to it anymore on my local DB

							System.err.println("Local purge of the document " + rep.getID() + "... ");
							try {
								database.purge(rep.getID());
							} catch (CouchbaseLiteException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							System.err.println("Local purge of the document " + rep.getID() + "... DONE !");
						}
					}
				}
			});

			// Start replication.
			replicator.start();

			// Check status of replication and wait till it is completed
//			while (replicator.getStatus().getActivityLevel() != ReplicatorActivityLevel.STOPPED) {
			while (true) { // inifinite loop
				Thread.sleep(5000);

				if (writeNewDocument) {
					createNewDoc(database);
				}

				System.out.println("Run compaction...");
				database.performMaintenance(MaintenanceType.COMPACT);
				System.out.println("Run compaction... DONE !");

				int numRows = 0;
				// Create a query to fetch all documents.
				System.out.println("== Executing Query 3");
				Query queryAll = QueryBuilder.select(SelectResult.expression(Meta.id), SelectResult.property(Prop_Name),
						SelectResult.property(Prop_TimeStamp), SelectResult.property(Prop_Type),
						SelectResult.property(Prop_Channels), SelectResult.property(Prop_AvgDev),
						SelectResult.property(Prop_Status)).from(DataSource.database(database));
				try {
					for (Result thisDoc : queryAll.execute()) {
						numRows++;
						System.out.println(String.format(
								"%d ... Id: %s iname: %s timestamp: %.2f type is %s channels: %s avg_dev: %.2f  status: %b",
								numRows, thisDoc.getString(Prop_Id), thisDoc.getString(Prop_Name),
								thisDoc.getDouble(Prop_TimeStamp), thisDoc.getString(Prop_Type),
								thisDoc.getString(Prop_Channels), thisDoc.getDouble(Prop_AvgDev),
								thisDoc.getBoolean(Prop_Status)));
					}
				} catch (CouchbaseLiteException e) {
					e.printStackTrace();
				}
				System.out.println(String.format("Total rows returned by query = %d", numRows));
			}

		} catch (Exception e) {
			System.err.println("Error !! -> Exception" + e);
		}

		System.exit(0);
	}

	private static void createNewDoc(Database database) throws CouchbaseLiteException {

		String channelValue = RANDOM.nextBoolean() == true ? "sensor_A_1" : "sensor_A_2";

		long ts = new Date().getTime();
		// Create a new document (i.e. a record) in the database.
		MutableDocument mutableDoc = new MutableDocument(channelValue + "::" + ts).setString(Prop_Type, "sensor")
				.setString(Prop_Channels, "sensor_A_1");

		// Save it to the database.
		database.save(mutableDoc);

		// Update a document.
		mutableDoc = database.getDocument(mutableDoc.getId()).toMutable();
		mutableDoc.setDouble(Prop_TimeStamp, ts);
		mutableDoc.setString(Prop_Name, "Capteur de pression");
		mutableDoc.setString(Prop_Channels, channelValue);
		mutableDoc.setString(Prop_Type, "sensor");
		mutableDoc.setDouble(Prop_AvgDev, RANDOM.nextDouble());
		mutableDoc.setBoolean(Prop_Status, RANDOM.nextBoolean());

		database.save(mutableDoc);

		Document document = database.getDocument(mutableDoc.getId());
		// Log the document ID (generated by the database) and properties
		System.out.println("Document ID is :: " + document.getId());
		System.out.println("Timestmap " + document.getDouble(Prop_TimeStamp));
		System.out.println("Name " + document.getString(Prop_Name));
		System.out.println("Channels " + document.getString(Prop_Channels));
		System.out.println("Type " + document.getString(Prop_Type));
		System.out.println("Type " + document.getDouble(Prop_AvgDev));
		System.out.println("Status " + document.getBoolean(Prop_Status));

	}

	/**
	 * Create the GUI and show it. For thread safety, this method should be invoked
	 * from the event-dispatching thread.
	 */
	private static void createAndShowGUI() {
		// Make sure we have nice window decorations.
		JFrame.setDefaultLookAndFeelDecorated(true);

		// Create and set up the window.
		JFrame frame = new JFrame("push / pull / pushAndPull TESTER");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setMinimumSize(new Dimension(500, 300));

		// panel.setBounds(61, 11, 81, 140);
		panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
		frame.getContentPane().add(panel);

		panel.add(cb);

		// Display the window.
		frame.pack();
		frame.setVisible(true);
	}
}