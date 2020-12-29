package org.flhy.ext;

import org.apache.commons.dbcp.BasicDataSource;
import org.flhy.ext.core.PropsUI;
import org.pentaho.di.core.DBCache;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.*;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.sql.DataSource;
import java.util.ArrayList;

public class App implements ApplicationContextAware {

	private static App app;
	public static KettleDatabaseRepositoryMeta meta;

	private LogChannelInterface log;
	private TransExecutionConfiguration transExecutionConfiguration;
	private TransExecutionConfiguration transPreviewExecutionConfiguration;
	private TransExecutionConfiguration transDebugExecutionConfiguration;
	private JobExecutionConfiguration jobExecutionConfiguration;
	public PropsUI props;

	private App() {
		props = PropsUI.getInstance();
		log = new LogChannel( PropsUI.getAppName());
		loadSettings();
		
		transExecutionConfiguration = new TransExecutionConfiguration();
	    transExecutionConfiguration.setGatheringMetrics( true );
	    transPreviewExecutionConfiguration = new TransExecutionConfiguration();
	    transPreviewExecutionConfiguration.setGatheringMetrics( true );
	    transDebugExecutionConfiguration = new TransExecutionConfiguration();
	    transDebugExecutionConfiguration.setGatheringMetrics( true );

	    jobExecutionConfiguration = new JobExecutionConfiguration();
	    
	    variables = new RowMetaAndData( new RowMeta() );
	}
	
	public void loadSettings() {
		LogLevel logLevel = LogLevel.getLogLevelForCode(props.getLogLevel());
		DefaultLogLevel.setLogLevel(logLevel);
		log.setLogLevel(logLevel);
		KettleLogStore.getAppender().setMaxNrLines(props.getMaxNrLinesInLog());

		// transMeta.setMaxUndo(props.getMaxUndo());
		DBCache.getInstance().setActive(props.useDBCache());
	}

	public static App getInstance() {
		if (app == null) {
			app = new App();
		}
		return app;
	}

	private Repository repository;

	public Repository getRepository() {
		return repository;
	}
	
	private Repository defaultRepository;
	
//	public void initDefault(Repository defaultRepo) {
//		if(this.defaultRepository == null)
//			this.defaultRepository = defaultRepo;
//		this.repository = defaultRepo;
//	}
	
	public Repository getDefaultRepository() {
		return this.defaultRepository;
	}
	
	public void selectRepository(Repository repo) {
		if(repository != null) {
			repository.disconnect();
		}
		repository = repo;
	}

	private DelegatingMetaStore metaStore;

	public DelegatingMetaStore getMetaStore() {
		return metaStore;
	}
	
	public LogChannelInterface getLog() {
		return log;
	}
	
	private RowMetaAndData variables = null;
	private ArrayList<String> arguments = new ArrayList<String>();
	
	public String[] getArguments() {
		return arguments.toArray(new String[arguments.size()]);
	}
	
	public JobExecutionConfiguration getJobExecutionConfiguration() {
		return jobExecutionConfiguration;
	}

	public TransExecutionConfiguration getTransDebugExecutionConfiguration() {
		return transDebugExecutionConfiguration;
	}

	public TransExecutionConfiguration getTransPreviewExecutionConfiguration() {
		return transPreviewExecutionConfiguration;
	}

	public TransExecutionConfiguration getTransExecutionConfiguration() {
		return transExecutionConfiguration;
	}
	
	public RowMetaAndData getVariables() {
		return variables;
	}

	@Override
	public void  setApplicationContext(ApplicationContext context) throws BeansException {
		KettleDatabaseRepository repository = new KettleDatabaseRepository();
		try {
			KettleEnvironment.init();
			PropsUI.init( "KettleWebConsole", Props.TYPE_PROPERTIES_KITCHEN );
			BasicDataSource dataSource = (BasicDataSource) context.getBean(DataSource.class);
		//	DatabaseMeta dbMeta = new DatabaseMeta();
//			Connection conn = dataSource.getConnection();
//			DatabaseMetaData dm = conn.getMetaData();
//			System.out.println(conn.getCatalog());
//			System.out.println(dm.getUserName());
//			System.out.println(dm.getURL());
//			System.out.println(dm.getDriverName());
//			System.out.println(dm.getDatabaseProductName());
//			System.out.println(dm.get);
			
//			String url = dataSource.getUrl();
//			String hostname = url.substring(url.indexOf("//") + 2, url.lastIndexOf(":"));
//			String port = url.substring(url.lastIndexOf(":") + 1, url.lastIndexOf("/"));
//			String dbName = url.substring(url.lastIndexOf("/") + 1);
//
//			dbMeta.setName("ucloud");
//			//dbMeta.setName("192.168.1.201_kettle");
//			dbMeta.setDBName("pentaho");
//			//dbMeta.setDBName(dbName);
//			dbMeta.setDatabaseType("MYSQL");
//			dbMeta.setAccessType(0);
////			dbMeta.setHostname(hostname);
////			dbMeta.setServername(hostname);
//			dbMeta.setHostname("ucloud");
//			dbMeta.setServername("ucloud");
////			dbMeta.setDBPort(port);
//			dbMeta.setDBPort("3306");
////			dbMeta.setUsername(dataSource.getUsername());
//			dbMeta.setUsername("root");
////			dbMeta.setPassword(dataSource.getPassword());
//			dbMeta.setPassword("12345");
//			ObjectId objectId = new LongObjectId(100);
//			dbMeta.setObjectId(objectId);
//			dbMeta.setShared(true);
//			dbMeta.addExtraOption(dbMeta.getPluginId(), "characterEncoding", "utf8");
//			dbMeta.addExtraOption(dbMeta.getPluginId(), "useUnicode", "true");
//			dbMeta.addExtraOption(dbMeta.getPluginId(), "autoReconnect", "true");
//			meta = new KettleDatabaseRepositoryMeta();
//			meta.setName("192.168.1.201_kettle");
//			meta.setId("KettleDatabaseRepository");
//			meta.setConnection(dbMeta);
//			meta.setDescription("192.168.1.201_kettle");
//
//			repository.init(meta);
//			repository.connect("admin", "admin");
			DatabaseMeta dbMeta = new DatabaseMeta();
			dbMeta.setName("defaultDatabase");
			dbMeta.setDBName("Pentaho");
			dbMeta.setDatabaseType("MySQL");
			dbMeta.setAccessType(0);
			dbMeta.setHostname("ucloud");
			dbMeta.setServername("ucloud");
			dbMeta.setDBPort("3306");
			dbMeta.setUsername("root");
			dbMeta.setPassword("12345");
			//KettleDatabaseRepositoryMeta meta = new KettleDatabaseRepositoryMeta();
			meta.setName("defaultRepository");
			meta.setId("defaultRepository");
			meta.setConnection(dbMeta);
			//KettleDatabaseRepository repository = new KettleDatabaseRepository();
			repository.init(meta);
			repository.connect("admin", "admin");
			App.getInstance().selectRepository(repository);
			if (App.getInstance().getRepository() == null ) {
				System.out.println("初始化资源库为空!!!!!!!!!!!!!!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

//	public JSONArray encodeVariables() {
//		Object[] data = variables.getData();
//		String[] fields = variables.getRowMeta().getFieldNames();
//		JSONArray jsonArray = new JSONArray();
//		for (int i = 0; i < fields.length; i++) {
//			JSONObject jsonObject = new JSONObject();
//			jsonObject.put("name", fields[i]);
//			jsonObject.put("value", data[i].toString());
//			jsonArray.add(jsonObject);
//		}
//		return jsonArray;
//	}
	
}