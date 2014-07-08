package zx.soft.zk.manager.domain;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeafBean implements Comparable<LeafBean> {

	private final static Logger logger = LoggerFactory.getLogger(LeafBean.class);
	private String path;
	private String name;
	private byte[] value;

	//	private String strValue;

	public LeafBean(String path, String name, byte[] value) {
		super();
		this.path = path;
		this.name = name;
		this.value = value;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public String getStrValue() {
		try {
			return new String(this.value, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			logger.error(ex.getMessage());
		}
		return null;
	}

	//	public void setStrValue(String strValue) {
	//		this.strValue = strValue;
	//	}

	@Override
	public int compareTo(LeafBean o) {
		return (this.path + this.name).compareTo((o.path + o.path));
	}

}
