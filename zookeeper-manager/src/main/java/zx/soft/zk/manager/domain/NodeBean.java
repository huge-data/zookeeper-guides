package zx.soft.zk.manager.domain;

public class NodeBean {

	private boolean isNew;
	private String name;
	private String value;
	private String description;

	public NodeBean() {
		super();
	}

	public NodeBean(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public boolean getIsNew() {
		return isNew;
	}

	public void setIsNew(boolean isNew) {
		this.isNew = isNew;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setNew(boolean isNew) {
		this.isNew = isNew;
	}

}
