package fr.fleray.couchbase.sample.cblite;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.couchbase.lite.BasicAuthenticator;
import com.couchbase.lite.Blob;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DataSource;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseChange;
import com.couchbase.lite.DatabaseChangeListener;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.Dictionary;
import com.couchbase.lite.Document;
import com.couchbase.lite.DocumentFlag;
import com.couchbase.lite.DocumentReplication;
import com.couchbase.lite.DocumentReplicationListener;
import com.couchbase.lite.Endpoint;
import com.couchbase.lite.Expression;
import com.couchbase.lite.MaintenanceType;
import com.couchbase.lite.MutableDocument;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryBuilder;
import com.couchbase.lite.QueryChange;
import com.couchbase.lite.QueryChangeListener;
import com.couchbase.lite.ReplicatedDocument;
import com.couchbase.lite.Replicator;
import com.couchbase.lite.ReplicatorActivityLevel;
import com.couchbase.lite.ReplicatorConfiguration;
import com.couchbase.lite.Result;
import com.couchbase.lite.ResultSet;
import com.couchbase.lite.SelectResult;
import com.couchbase.lite.URLEndpoint;

public class GettingStartedWithAttachments {

	private static boolean deleteDoc = true;
	private static int docWithAttachToCreate = 1;
	private static String[] docIdToDelete = new String[] { "" };
//	private static String[] docIdToDelete = new String[] {"ATTAC_0eccb2cf-f8e9-4b54-84fe-e2f5f2790f56"};
	private static final String DB_NAME = "french_cuisine";
	/*
	 * Credentials declared this way purely for expediency in this demo - use OAUTH
	 * in production code
	 */
	private static final String DB_USER = "pierre"; // (Bretagne) OR wolfgang (Alsace) marius (PACA)
	private static final String DB_PASS = "password";

	private static final String SYNC_GATEWAY_URL = "ws://localhost:4984/french_cuisine"; // "ws://52.174.108.107:4984/stime";
	private static final String DB_PATH = new File("").getAbsolutePath() + "/resources";

	private static final String ZIP_FOLDER = "/Users/fabriceleray/eclipse-workspace/EDF_helloMobile/input/";

	public static void main(String[] args) throws CouchbaseLiteException, InterruptedException, URISyntaxException {

		String Prop_Title = "titre";
		String Prop_Author = "auteur";

		String Prop_Type = "type";
		String searchStringType = "rapport";

		String Prop_Channels = "channels";
		String channelValue = "PDV_Bretagne";

		// Initialize Couchbase Lite
		CouchbaseLite.init();

		// Get the database (and create it if it doesn’t exist).
		DatabaseConfiguration config = new DatabaseConfiguration();
		config.setDirectory(DB_PATH);
		// config.setEncryptionKey(new EncryptionKey(DB_PASS));
		Database database = new Database(DB_NAME, config);

		database.addChangeListener(new DatabaseChangeListener() {

			@Override
			public void changed(DatabaseChange change) {
				System.out.println("Database change for docIds:");
				for (String docId : change.getDocumentIDs()) {
					System.out.println("-> " + docId);
				}
			}
		});
		

		for (int i = 0; i < docWithAttachToCreate; i++) {
			// Create a new document (i.e. a record) in the database.
			MutableDocument mutableDoc = new MutableDocument("petit_dej_0" + (i + 1)).setString(Prop_Type, searchStringType);

			// Save it to the database.
			database.save(mutableDoc);

			// Update a document.
			mutableDoc = database.getDocument(mutableDoc.getId()).toMutable();
			System.out.println(mutableDoc.getRevisionID());

			mutableDoc.setString(Prop_Title, "Titre défini depuis le Mobile");
			mutableDoc.setString(Prop_Author, "Auteur depuis le Mobile");
			mutableDoc.setString(Prop_Channels, channelValue);

			// Save it to the database.
			database.save(mutableDoc);

			// add attachment
//			System.out.println(ClassLoader.class.getResourceAsStream("/avatar2.jpg").toString());
			InputStream is = ClassLoader.class.getResourceAsStream("/petit_dej.png");
			if (is == null) {
				return;
			}
			try {
				Blob blob = new Blob("image/png", is);
				mutableDoc.setBlob("petit_dej", blob);

				database.save(mutableDoc);
			} finally {
				try {
					is.close();
				} catch (IOException ignore) {
					System.err.println(ignore);
				}
			}
			

//			if (deleteDoc) {
//				
//				database.delete(mutableDoc);
//			}
//			
//			
//			// remove unused attachments :
//			try {
//				// from javadoc : compacts the database file by deleting unused
//				// attachment files and vacuuming the SQLite database
//
//				// see also :
//				// https://forums.couchbase.com/t/delete-a-document-with-attachments/11468
//				database.performMaintenance(MaintenanceType.COMPACT);
//			} catch (CouchbaseLiteException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//			// add 2nd attachment
//			is = ClassLoader.class.getResourceAsStream("/saturn2.jpg");
//			if (is == null) {
//				return;
//			}
//			try {
//				Blob blob = new Blob("image/jpeg", is);
//				mutableDoc.setBlob("saturn2", blob);
//
//				database.save(mutableDoc);
//			} finally {
//				try {
//					is.close();
//				} catch (IOException ignore) {
//					System.err.println(ignore);
//				}
//			}

//			Document document = database.getDocument(mutableDoc.getId());
//			// Log the document ID (generated by the database) and properties
//			System.out.println("Document ID is :: " + document.getId());
//			System.out.println("Titre " + document.getString(Prop_Title));
//			System.out.println("Auteur " + document.getDouble(Prop_Author));
//			System.out.println("Channels " + document.getString(Prop_Channels));
		}

		for (String docToDelete : docIdToDelete) {
			if (!docToDelete.isEmpty()) {
				Document doc = database.getDocument(docToDelete);
				database.delete(doc);
			}
		}

		// Create a query to fetch documents of type "product".
		System.out.println("== Executing Query 1");
		Query query = QueryBuilder.select(SelectResult.all()).from(DataSource.database(database))
				.where(Expression.property(Prop_Type).equalTo(Expression.string(searchStringType)));
		ResultSet result = query.execute();
		System.out.println(
				String.format("Query returned %d rows of type %s", result.allResults().size(), searchStringType));

		// Live Query
		// Java :
		// https://docs.couchbase.com/couchbase-lite/current/java-platform.html#live-query
		// Android :
		// https://docs.couchbase.com/couchbase-lite/current/java-android.html#live-query
		query.addChangeListener(new QueryChangeListener() {

			@Override
			public void changed(QueryChange change) {
				ResultSet results = change.getResults();

				for (Result res : results) {
					Iterator<String> it = res.iterator();
					while (it.hasNext()) {
						String next = it.next();
						System.out.println(next);

						Dictionary dict = res.getDictionary(next);

						List<String> keys = dict.getKeys();
						for (String k : keys) {
							Map<String, byte[]> zipfiles = new HashMap<String, byte[]>();
							Object obj = dict.getValue(k);
							if (obj instanceof String) {
								System.out.println("-> KEY: " + k + "\t VALUE: " + (String) obj);
							} else if (obj instanceof Dictionary) {
								Dictionary dict2 = (Dictionary) obj;
								for (String blobname : dict2.getKeys()) {
									Object obj2 = dict2.getValue(blobname);

									if (obj2 instanceof String) {
										System.out.println("-> KEY: " + k + "\t VALUE: " + (String) obj2);
									} else if (obj2 instanceof Blob) {
										Blob blob = (Blob) obj2;
										byte[] content = blob.getContent();

										zipfiles.put(blobname, content);

										System.out.println("Bloc content type = " + blob.getContentType());
										System.out.println("Bloc length = " + content.length);
										// we have blobs => let's get their value and rebuild the image.
										// System.out.println("-> KEY: " + k + "\t VALUE: " + (String)
										// content.toString());
									}
								}
							}

						}
					}
				}
			}
		});

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
			}
		});

		replicator.addDocumentReplicationListener(new DocumentReplicationListener() {

			@Override
			public void replication(DocumentReplication documentReplication) {
				for (ReplicatedDocument rep : documentReplication.getDocuments()) {

					CouchbaseLiteException err = rep.getError();
					if (err != null) {
						// There was an error
						System.err.println("Error replicating/deleting document: " + err);
						return;
					}

//					if (rep.flags().contains(DocumentFlag.DELETED)) {
//						System.err
//								.println("Successfully replicated the deletion of document for docID = " + rep.getID());
//
//						// remove unused attachments :
//						try {
//							// from javadoc : compacts the database file by deleting unused
//							// attachment files and vacuuming the SQLite database
//
//							// see also :
//							// https://forums.couchbase.com/t/delete-a-document-with-attachments/11468
//							database.performMaintenance(MaintenanceType.COMPACT);
//						} catch (CouchbaseLiteException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//
//						// TODO : F.LERAY in case the deletion happens after the sync,
//						// force the deletion of the "rebuild doc".
//
//					} else {
//						System.err.println("Document " + rep.getID() + " has been replicated !!");
//					}
				}
			}
		});

		// Start replication.
		replicator.start();

		// Check status of replication and wait till it is completed
		while (replicator.getStatus().getActivityLevel() != ReplicatorActivityLevel.STOPPED) {
			Thread.sleep(2000);
		}

		System.out.println("Finish!");

		System.exit(0);
	}

}