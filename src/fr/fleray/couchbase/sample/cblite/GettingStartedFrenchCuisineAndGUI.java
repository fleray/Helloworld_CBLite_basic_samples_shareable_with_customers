package fr.fleray.couchbase.sample.cblite;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.swing.Action;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.couchbase.lite.AbstractReplicatorConfiguration.ReplicatorType;
import com.couchbase.lite.BasicAuthenticator;
import com.couchbase.lite.ConcurrencyControl;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DataSource;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.Document;
import com.couchbase.lite.DocumentFlag;
import com.couchbase.lite.DocumentReplication;
import com.couchbase.lite.DocumentReplicationListener;
import com.couchbase.lite.Endpoint;
import com.couchbase.lite.Expression;
import com.couchbase.lite.IndexBuilder;
import com.couchbase.lite.LogDomain;
import com.couchbase.lite.LogLevel;
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

public class GettingStartedFrenchCuisineAndGUI {

	private static boolean writeNewDocument = false;
	private static Database database;
	private static final String DB_NAME = "french_cuisine";
	/*
	 * Credentials declared this way purely for expediency in this demo - use OAUTH
	 * in production code
	 */
	private static final String SYNC_GATEWAY_URL = "ws://localhost:4984/french_cuisine"; // "ws://52.174.108.107:4984/stime";
//	private static final String SYNC_GATEWAY_URL = "ws://localhost:3000/french_cuisine"; // "ws://52.174.108.107:4984/stime";

	private static final String DB_PATH = new File("").getAbsolutePath() + "/resources";

	private final static List<Replicator> REPLICATOR_LIST = new ArrayList<Replicator>();

	private static String docID = "my_test_doc";

	private static JButton addButton = new JButton("Add DocID...");
	private static JButton getButton = new JButton("Get DocID...");
	private static JButton deleteButton = new JButton("Delete DocID...");

	private static Document my_get_document;

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

		JPanel panel = new JPanel();

		// panel.setBounds(61, 11, 81, 140);
		panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
		frame.getContentPane().add(panel);

		JLabel label = new JLabel("# replicators running : " + REPLICATOR_LIST.size());
		panel.add(label);

		Document document = database.getDocument(docID);
		JLabel label2 = null;
		if (null == document) {
			label2 = new JLabel("# Document ID 'my_test_doc' does not exist inside local DB");
		} else {
			label2 = new JLabel("# Document ID my_test_doc's content : " + document.toString());
		}
		panel.add(label2);

		String channel = "Bretagne_region";
		ReplicatorConfiguration.ReplicatorType repType = ReplicatorConfiguration.ReplicatorType.PULL;
		createNewReplicationButton(panel, label, channel, repType);

		channel = "Bretagne_region";
		repType = ReplicatorConfiguration.ReplicatorType.PUSH;
		createNewReplicationButton(panel, label, channel, repType);

		channel = "Alsace_region";
		repType = ReplicatorConfiguration.ReplicatorType.PULL;
		createNewReplicationButton(panel, label, channel, repType);

		channel = "Alsace_region";
		repType = ReplicatorConfiguration.ReplicatorType.PUSH;
		createNewReplicationButton(panel, label, channel, repType);

		channel = "Bretagne_region";
		repType = ReplicatorConfiguration.ReplicatorType.PUSH_AND_PULL;
		createNewReplicationButton(panel, label, channel, repType);

		upsertDocumentButton(panel, label2, docID);

		JLabel label3 = new JLabel("New DocID to create/update: ");
		JTextField textField3 = new JTextField("docID_" + UUID.randomUUID());

		textField3.getDocument().addDocumentListener(new DocumentListener() {

			private void update() {
				deleteButton.setText("Delete DocID " + textField3.getText());
				addButton.setText("Upsert DocID " + textField3.getText());
				getButton.setText("Get DocID " + textField3.getText());
			}

			@Override
			public void removeUpdate(DocumentEvent e) {
				update();
			}

			@Override
			public void insertUpdate(DocumentEvent e) {
				update();
			}

			@Override
			public void changedUpdate(DocumentEvent e) {
				update();
			}
		});

		panel.add(label3);
		panel.add(textField3);

		getDocumentButton(panel, textField3);

		upsertDocumentButton(panel, textField3);

		deleteDocumentButton(panel, textField3);

		// Display the window.
		frame.pack();
		frame.setVisible(true);
	}

	private static void getDocumentButton(JPanel panel, JTextField textField3) {

		getButton = new JButton(new Action() {

			@Override
			public void actionPerformed(ActionEvent e) {
				String myDocID = textField3.getText();
				my_get_document = null;
				Document doc = database.getDocument(myDocID);

				MutableDocument mutableDocument = null;
				if (doc == null) {
					JOptionPane.showMessageDialog(panel, "DocID " + myDocID + " does not exist!!");
					return;
				}

				mutableDocument = doc.toMutable();

				int val = doc.getInt("counter") + 1;
				mutableDocument.setInt("counter", val);

				System.out.println(" mutableDocument.getValue(\"counter\") = " + mutableDocument.getValue("counter"));

				my_get_document = doc;

			}

			@Override
			public Object getValue(String key) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void putValue(String key, Object value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void setEnabled(boolean b) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isEnabled() {
				// TODO Auto-generated method stub
				return true;
			}

			@Override
			public void addPropertyChangeListener(PropertyChangeListener listener) {
				// TODO Auto-generated method stub

			}

			@Override
			public void removePropertyChangeListener(PropertyChangeListener listener) {
				// TODO Auto-generated method stub

			}

		});

		getButton.setText("Get docID '" + textField3.getText() + "'");
		panel.add(getButton);

	}

	private static void upsertDocumentButton(JPanel panel, JTextField textField3) {

		addButton = new JButton(new Action() {

			@Override
			public void actionPerformed(ActionEvent e) {
				String myDocID = textField3.getText();
				Document doc = database.getDocument(myDocID);

				MutableDocument mutableDocument = null;
				if (my_get_document != null) {
					mutableDocument = my_get_document.toMutable();
				} else if (doc == null) {
					mutableDocument = new MutableDocument(myDocID);
					mutableDocument.setString("channels", "Bretagne_region");
					mutableDocument.setString("type", "product");
					mutableDocument.setDouble("price", 1.5);
					mutableDocument.setInt("counter", 0);
				} else {
					mutableDocument = doc.toMutable();

					int val = doc.getInt("counter") + 1;
					mutableDocument.setInt("counter", val);

					System.out
							.println(" mutableDocument.getValue(\"counter\") = " + mutableDocument.getValue("counter"));
				}

				try {
					String revisionID = null;
					if (my_get_document != null) {
						revisionID = my_get_document.getRevisionID();
					}
					System.out.println("BEFORE UPDATE: revisionID = " + revisionID);

					boolean saveOK = database.save(mutableDocument, ConcurrencyControl.FAIL_ON_CONFLICT);
					System.out.println("saveOK = " + saveOK);
					if (!saveOK) {
						// save was a NOT success. mutableDocument is the new saved revision.
						// Here : force the new version to be saved. See https://blog.couchbase.com/document-conflicts-couchbase-mobile/
						database.save(mutableDocument);
					}
					// currenDoc is the "current saved version" of doc
					Document currenDoc = database.getDocument(mutableDocument.getId());
					revisionID = currenDoc.getRevisionID();
					System.out.println("AFTER UPDATE: revisionID = " + revisionID);
				} catch (CouchbaseLiteException ee) {
					// TODO Auto-generated catch block
					ee.printStackTrace();
				}
			}

			@Override
			public Object getValue(String key) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void putValue(String key, Object value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void setEnabled(boolean b) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isEnabled() {
				// TODO Auto-generated method stub
				return true;
			}

			@Override
			public void addPropertyChangeListener(PropertyChangeListener listener) {
				// TODO Auto-generated method stub

			}

			@Override
			public void removePropertyChangeListener(PropertyChangeListener listener) {
				// TODO Auto-generated method stub

			}

		});

		addButton.setText("Upsert docID '" + textField3.getText() + "'");
		panel.add(addButton);

	}

	private static void deleteDocumentButton(JPanel panel, JTextField textField3) {

		deleteButton.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				String myDocID = textField3.getText();
				deleteButton.setText("Delete docID '" + textField3.getText() + "'");

				Document doc = database.getDocument(myDocID);

				MutableDocument mutableDocument = null;
				if (doc == null) {
					System.err.println("doc to delete is NULL (not found)");
				} else {
					try {
						database.delete(doc);

						Query queryAll = QueryBuilder
								.select(SelectResult.expression(Meta.id), 
										SelectResult.expression(Meta.deleted),
										SelectResult.expression(Meta.revisionID),
										SelectResult.expression(Meta.sequence))
								.from(DataSource.database(database))
								.where(Meta.deleted.equalTo(Expression.booleanValue(true)));

						
						for (Result thisRes : queryAll.execute()) {
							System.out.println(
									"Meta.id: " + thisRes.getString(0) + "\t Meta.deleted: " + thisRes.getBoolean(1));
						}
					} catch (CouchbaseLiteException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}

		});

		deleteButton.setText("Delete docID '" + textField3.getText() + "'");
		panel.add(deleteButton);
	}

	private static void upsertDocumentButton(JPanel panel, JLabel label2, String docID) {

		JButton button = new JButton(new Action() {

			@Override
			public void actionPerformed(ActionEvent e) {
				Document doc = database.getDocument(docID);

				MutableDocument mutableDocument = null;
				if (doc == null) {
					mutableDocument = new MutableDocument(docID);
					mutableDocument.setString("channels", "Bretagne_region");
					mutableDocument.setString("type", "product");
					mutableDocument.setDouble("price", 1.5);
					mutableDocument.setInt("counter", 0);
				} else {
					mutableDocument = doc.toMutable();

					int val = doc.getInt("counter") + 1;
					mutableDocument.setInt("counter", val);

					System.out
							.println(" mutableDocument.getValue(\"counter\") = " + mutableDocument.getValue("counter"));
				}

				try {
					database.save(mutableDocument);
				} catch (CouchbaseLiteException ee) {
					// TODO Auto-generated catch block
					ee.printStackTrace();
				}

				label2.setText("# Document ID my_test_doc's content : " + mutableDocument.toString());

			}

			@Override
			public Object getValue(String key) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void putValue(String key, Object value) {
				// TODO Auto-generated method stub

			}

			@Override
			public void setEnabled(boolean b) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isEnabled() {
				// TODO Auto-generated method stub
				return true;
			}

			@Override
			public void addPropertyChangeListener(PropertyChangeListener listener) {
				// TODO Auto-generated method stub

			}

			@Override
			public void removePropertyChangeListener(PropertyChangeListener listener) {
				// TODO Auto-generated method stub

			}

		});

		button.setText("Upsert docID '" + docID + "'");
		panel.add(button);
	}

	private static void createNewReplicationButton(JPanel panel, JLabel label, String channel, ReplicatorType repType) {

		JButton button = new JButton(new Action() {

			@Override
			public void actionPerformed(ActionEvent e) {

				ReplicatorConfiguration replConfig = null;
				try {
					Endpoint targetEndpoint = new URLEndpoint(new URI(SYNC_GATEWAY_URL));
					replConfig = new ReplicatorConfiguration(database, targetEndpoint);
				} catch (URISyntaxException ee) {
					// TODO Auto-generated catch block
					ee.printStackTrace();
				}

				replConfig.setReplicatorType(repType);
				replConfig.setContinuous(true);

				// filter by channel

				replConfig.setChannels(Collections.singletonList(channel));

				// Add authentication.
				replConfig.setAuthenticator(new BasicAuthenticator("admin", "password".toCharArray()));

				Replicator replicator = new Replicator(replConfig);

				// Listen to replicator change events.
				replicator.addChangeListener(change -> {
					if (change.getStatus().getError() != null) {
						System.err.println("Error code ::  " + change.getStatus().getError().getCode());
						System.err.println("XXXXXXXXXX ::  " + new Date());
					}

					// if (change.getStatus().getActivityLevel() == ActivityLevel.
				});

				replicator.addDocumentReplicationListener(new DocumentReplicationListener() {

					@Override
					public void replication(DocumentReplication documentReplication) {
						for (ReplicatedDocument rep : documentReplication.getDocuments()) {
							System.err.println("Document " + rep.getID() + " has been replicated !!");

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
									if (database.getDocument(rep.getID()) != null) {
										database.purge(rep.getID());
									}
								} catch (CouchbaseLiteException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								System.err.println("Local purge of the document " + rep.getID() + "... DONE !");
							}
						}

						Query queryAll = null;
						
						queryAll = QueryBuilder.select(SelectResult.expression(Meta.id))
								.from(DataSource.database(database));
						
						try {
//							queryAll = QueryBuilder.createQuery(
//									"SELECT meta().id FROM _", database);
							
							database.createQuery("SELECT meta().id FROM _");
						} catch (CouchbaseLiteException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						
						int numRows = 0;
						try {
							for (Result thisDoc : queryAll.execute()) {
								numRows++;
							}
						} catch (CouchbaseLiteException e) {
							e.printStackTrace();
						}
						System.out.println(
								String.format("END REPLICAITON => Total rows returned by query = %d", numRows));
					}
				});

				// Start replication.
				replicator.start();

				REPLICATOR_LIST.add(replicator);

				label.setText("# replicators running : " + REPLICATOR_LIST.size());

				System.out.println("# replicators running : " + REPLICATOR_LIST.size());

			}

			@Override
			public Object getValue(String key) {
				return null;
			}

			@Override
			public void putValue(String key, Object value) {
			}

			@Override
			public void setEnabled(boolean b) {
			}

			@Override
			public boolean isEnabled() {
				return true;
			}

			@Override
			public void addPropertyChangeListener(PropertyChangeListener listener) {

			}

			@Override
			public void removePropertyChangeListener(PropertyChangeListener listener) {
			}

		});

		button.setText(channel + " | " + repType.toString());
		panel.add(button);

	}

	public static void main(String[] args) {

		try {
			Random RANDOM = new Random();

			Double randVn = RANDOM.nextDouble() + 1;

			String Prop_Id = "id";
			String Prop_Name = "name";
			String Prop_Price = "price";

			String Prop_Type = "type";
			String searchStringType = "product";

			String Prop_Channels = "channels";
			String channelValue = "Bretagne_region";

			// Initialize Couchbase Lite
			CouchbaseLite.init();
			
			// Database.setLogLevel(LogDomain.REPLICATOR, LogLevel.VERBOSE);

			// Get the database (and create it if it doesn’t exist).
			DatabaseConfiguration config = new DatabaseConfiguration();
//			config.setEncryptionKey(encryptionKey)
			config.setDirectory(DB_PATH);
			// config.setEncryptionKey(new EncryptionKey(DB_PASS));
			database = new Database(DB_NAME, config);
			
			List<String> indexes = database.getIndexes();

			// Create index on 
            if (!indexes.contains("MyIndexOnChannels")) {

                database.createIndex("MyIndexOnChannels",
                        IndexBuilder.valueIndex(ValueIndexItem.property("channels")));
            }


			if (writeNewDocument) {
				// Create a new document (i.e. a record) in the database.
				MutableDocument mutableDoc = new MutableDocument("produit_from_CBL_" + UUID.randomUUID())
						.setString(Prop_Type, "product").setString(Prop_Channels, "Alsace_region");
				
				
				// Save it to the database.
				database.save(mutableDoc);

				// Update a document.
				mutableDoc = database.getDocument(mutableDoc.getId()).toMutable();
				mutableDoc.setDouble(Prop_Price, randVn);
				mutableDoc.setString(Prop_Name, "produit_local_DB");
				mutableDoc.setString(Prop_Channels, channelValue);
				database.save(mutableDoc);

				Document document = database.getDocument(mutableDoc.getId());
				// Log the document ID (generated by the database) and properties
				System.out.println("Document ID is :: " + document.getId());
				System.out.println("Name " + document.getString(Prop_Name));
				System.out.println("Price " + document.getDouble(Prop_Price));
				System.out.println("Channels " + document.getString(Prop_Channels));
			}

			// Create a query to fetch documents of type "product".
			System.out.println("== Executing Query 1");
			Query query = QueryBuilder.select(SelectResult.all()).from(DataSource.database(database))
					.where(Expression.property(Prop_Type).equalTo(Expression.string(searchStringType)));
			ResultSet result = query.execute();
			System.out.println(
					String.format("Query returned %d rows of type %s", result.allResults().size(), searchStringType));
//
//			// Check status of replication and wait till it is completed
//			while (replicator.getStatus().getActivityLevel() != Replicator.ActivityLevel.STOPPED) {
//				Thread.sleep(5000);
//
//				int numRows = 0;
//				// Create a query to fetch all documents.
//				System.out.println("== Executing Query 3");
//				Query queryAll = QueryBuilder.select(SelectResult.expression(Meta.id), SelectResult.property(Prop_Name),
//						SelectResult.property(Prop_Price), SelectResult.property(Prop_Type),
//						SelectResult.property(Prop_Channels)).from(DataSource.database(database));
//				try {
//					for (Result thisDoc : queryAll.execute()) {
//						numRows++;
//						System.out.println(String.format("%d ... Id: %s is learning: %s version: %.2f type is %s",
//								numRows, thisDoc.getString(Prop_Id), thisDoc.getString(Prop_Name),
//								thisDoc.getDouble(Prop_Price), thisDoc.getString(Prop_Type)));
//					}
//				} catch (CouchbaseLiteException e) {
//					e.printStackTrace();
//				}
//				System.out.println(String.format("Total rows returned by query = %d", numRows));
//			}

		} catch (Exception e) {
			System.err.println("Error !! -> Exception" + e);
		}

		// Schedule a job for the event-dispatching thread:
		// creating and showing this application's GUI.
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});

		while (REPLICATOR_LIST.size() > -1) {
			try {
				Thread.sleep(5000);

				Query queryAll = QueryBuilder.select(SelectResult.expression(Meta.id), 
						SelectResult.expression(Meta.deleted),
						SelectResult.expression(Meta.revisionID),
						SelectResult.expression(Meta.sequence))
						.from(DataSource.database(database));
				int numRows = 0;
				try {
					for (Result thisDoc : queryAll.execute()) {
						numRows++;
					}
				} catch (CouchbaseLiteException e) {
					e.printStackTrace();
				}
				System.out.println(String.format("Total rows returned by query = %d", numRows));

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("Finish!");

		// System.exit(0);
	}
}