package zx.soft.zk.manager.domain;

import java.util.Date;

import org.javalite.activejdbc.Model;

public class History extends Model {

	private Long id;
	private String changeUser;
	private Date changeDate;
	private String changeSummary;
	private String changeIp;

	@Override
	public Long getId() {
		this.id = super.getLong("ID");
		return id;
	}

	public void setId(Long id) {
		super.setLong("ID", id);
	}

	public String getChangeUser() {
		this.changeUser = super.getString("CHANGE_USER");
		return changeUser;
	}

	public void setChangeUser(String changeUser) {
		super.setString("CHANGE_USER", changeUser);
	}

	public Date getChangeDate() {
		this.changeDate = super.getTimestamp("CHANGE_DATE");
		return changeDate;
	}

	public void setChangeDate(Date changeDate) {
		super.setTimestamp("CHANGE_DATE", changeDate);
	}

	public String getChangeSummary() {
		this.changeSummary = super.getString("CHANGE_SUMMARY");
		return changeSummary;
	}

	public void setChangeSummary(String changeSummary) {
		super.setString("CHANGE_SUMMARY", changeSummary);
	}

	public String getChangeIp() {
		this.changeIp = super.getString("CHANGE_IP");
		return changeIp;
	}

	public void setChangeIp(String changeIp) {
		super.setString("CHANGE_IP", changeIp);
	}

}
