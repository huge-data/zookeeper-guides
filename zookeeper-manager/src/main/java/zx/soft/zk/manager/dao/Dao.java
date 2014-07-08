package zx.soft.zk.manager.dao;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.javalite.activejdbc.Base;
import org.slf4j.LoggerFactory;

import zx.soft.zk.manager.domain.History;

import com.googlecode.flyway.core.Flyway;

public class Dao {

	private final static Integer FETCH_LIMIT = 50;
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(Dao.class);
	private final Properties globalProps;

	public Dao(Properties globalProps) {
		this.globalProps = globalProps;
	}

	public void open() {
		Base.open(globalProps.getProperty("jdbcClass"), globalProps.getProperty("jdbcUrl"),
				globalProps.getProperty("jdbcUser"), globalProps.getProperty("jdbcPwd"));
	}

	public void close() {
		Base.close();
	}

	public void checkNCreate() {

		Flyway flyway = new Flyway();
		flyway.setDataSource(globalProps.getProperty("jdbcUrl"), globalProps.getProperty("jdbcUser"),
				globalProps.getProperty("jdbcPwd"));
		//Will wipe db each time. Avoid this in prod.
		if (globalProps.getProperty("env").equals("dev")) {
			flyway.clean();
		}
		//Remove the above line if deploying to prod.
		flyway.migrate();
	}

	public List<History> fetchHistoryRecords() {
		this.open();
		List<History> history = History.findAll().orderBy("ID desc").limit((int) FETCH_LIMIT);
		history.size();
		this.close();
		return history;

	}

	public List<History> fetchHistoryRecordsByNode(String historyNode) {
		this.open();
		// List<History> history = History.where("CHANGE_SUMMARY like ?", historyNode).orderBy("ID desc").limit(FETCH_LIMIT);
		List<History> history = History.where("CHANGE_SUMMARY like ?", historyNode).orderBy("ID desc")
				.limit((int) FETCH_LIMIT);
		history.size();
		this.close();
		return history;
	}

	public void insertHistory(String user, String ipAddress, String summary) {
		try {
			this.open();
			//To avoid errors due to truncation.
			if (summary.length() >= 500) {
				summary = summary.substring(0, 500);
			}
			History history = new History();
			history.setChangeUser(user);
			history.setChangeIp(ipAddress);
			history.setChangeSummary(summary);
			history.setChangeDate(new Date());
			history.save();
			this.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
	}

}
