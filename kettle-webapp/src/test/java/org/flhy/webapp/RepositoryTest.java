package org.flhy.webapp;

import org.apache.ibatis.session.SqlSession;
import org.flhy.ext.App;
import org.flhy.ext.core.PropsUI;
import org.flhy.ext.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.sxdata.jingwei.entity.TaskGroupAttributeEntity;
import org.sxdata.jingwei.util.CommonUtil.StringDateUtil;
import org.sxdata.jingwei.util.TaskUtil.CarteClient;

import java.io.IOException;
import java.util.Date;

public class RepositoryTest {

	@Before
	public void getDatabaseRepository() throws KettleException {
		KettleEnvironment.init();
		PropsUI.init( "KettleWebConsole", Props.TYPE_PROPERTIES_KITCHEN );
		DatabaseMeta dbMeta = new DatabaseMeta();
		dbMeta.setName("defaultDatabase");
		dbMeta.setDBName("pentaho");
		dbMeta.setDatabaseType("MySQL");
		dbMeta.setAccessType(0);
		dbMeta.setHostname("ucloud");
		dbMeta.setServername("ucloud");
		dbMeta.setDBPort("3306");
		dbMeta.setUsername("root");
		dbMeta.setPassword("12345");
		KettleDatabaseRepositoryMeta meta = new KettleDatabaseRepositoryMeta();
		meta.setName("defaultRepository");
		meta.setId("defaultRepository");
		meta.setConnection(dbMeta);
		KettleDatabaseRepository repository = new KettleDatabaseRepository();
		repository.init(meta);
		repository.connect("admin", "admin");
		App.getInstance().selectRepository(repository);
	}
	
	@Test
	public void getRootNode() throws KettleException {
		Repository repository = App.getInstance().getRepository();
		RepositoryDirectoryInterface path = repository.findDirectory("/");
		System.out.println(path);
//		RepositoryDirectoryInterface newdir = repository.createRepositoryDirectory(path, "测试");
//		System.out.println(path.getSubdirectory(0));
	}

	// create trans
	@Test
	public void createTrans() throws KettleException, IOException {
		boolean isSuccess=false;
		String dir = "/";
		String transName = "james";
		String[] taskGroupArray = {"Pentaho"};
		Repository repository = App.getInstance().getRepository();
		RepositoryDirectoryInterface directory = null;
		TransMeta transMeta = null;
		try {
			directory = repository.findDirectory(dir);
			if(directory == null)
				directory = repository.getUserHomeDirectory();
			if(repository.exists(transName, directory, RepositoryObjectType.TRANSFORMATION)) {
				JsonUtils.fail("该转换已经存在，请重新输入！");
				return;
			}
			transMeta = new TransMeta();
			transMeta.setRepository(App.getInstance().getRepository());
			transMeta.setMetaStore(App.getInstance().getMetaStore());
			transMeta.setName(transName);
			transMeta.setRepositoryDirectory(directory);
			repository.save(transMeta, "add: " + new Date(), null);
			isSuccess=true;
			//添加任务组记录
			if(null!=taskGroupArray && taskGroupArray.length>0){
				Integer taskId=Integer.valueOf(transMeta.getObjectId().getId());
				for(String taskGroupName:taskGroupArray){
					if(StringDateUtil.isEmpty(taskGroupName)){
						continue;
					}
					TaskGroupAttributeEntity attr=new TaskGroupAttributeEntity();
					attr.setTaskGroupName(taskGroupName);
					attr.setType("trans");
					attr.setTaskId(taskId);
					attr.setTaskPath(dir + transName);
					attr.setTaskName(transName);
				//	sqlSession.insert("org.sxdata.jingwei.dao.TaskGroupDao.addTaskGroupAttribute",attr);
				}
//				sqlSession.commit();
//				sqlSession.close();
			}
			String transPath = directory.getPath();
			if(!transPath.endsWith("/"))
				transPath = transPath + '/';
			transPath = transPath + transName;
			//JsonUtils.success(transPath);
			System.out.println("创建成功");
		} catch (Exception e) {
			//出现异常回滚
			e.printStackTrace();
			if(e instanceof KettleException){
				repository.disconnect();
				repository.init( App.getInstance().meta);
				repository.connect("admin", "admin");
			}
//			sqlSession.rollback();
//			sqlSession.close();
			//删除转换
			if(isSuccess){
				ObjectId id = repository.getTransformationID(transName, directory);
				repository.deleteTransformation(id);
			}
			System.out.println(e.getStackTrace().toString());
			//JsonUtils.fail("创建失败!");
			System.out.println("创建失败");
		}
	}
	
}
