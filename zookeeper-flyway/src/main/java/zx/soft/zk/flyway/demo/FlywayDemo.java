package zx.soft.zk.flyway.demo;

import org.flywaydb.core.Flyway;

public class FlywayDemo {

	public static void main(String[] args) {

		// 创建Flyway实例
		Flyway flyway = new Flyway();

		// 指定到数据库
		flyway.setDataSource("jdbc:h2:file:distDB/testdb", "zk_manager", null);

		// 开始迁移
		flyway.migrate();

	}

}
