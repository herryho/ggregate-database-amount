import { Client } from "pg";

// 数据库连接配置
const config1 = {
  // 第一个数据库的配置
  user: "postgres",
  host: "127.0.0.1",
  database: "postgres",
  password: "postgres",
  port: 5432,
};

const config2 = {
  // 第二个数据库的配置
  user: "postgres",
  host: "127.0.0.1",
  database: "postgres",
  password: "postgres",
  port: 15432,
};

// 创建客户端
const client1 = new Client(config1);
const client2 = new Client(config2);

async function main() {
  try {
    await client1.connect();
    await client2.connect();

    // 查询两个数据库
    const query1 = "SELECT account, amount FROM crowdloans";
    const query2 = "SELECT account, amount FROM crowdloans";

    const res1 = await client1.query(query1);
    const res2 = await client2.query(query2);

    // 打印两个结果的长度
    console.log("Result 1 length:", res1.rows.length);
    console.log("Result 2 length:", res2.rows.length);

    // 合并结果
    const combinedResults = new Map<string, bigint>();
    res1.rows.forEach((row) => {
      const amount = BigInt(row.amount);
      const currentAmount = combinedResults.get(row.account) ?? BigInt(0);
      combinedResults.set(row.account, currentAmount + amount);
    });
    res2.rows.forEach((row) => {
      const amount = BigInt(row.amount);
      const currentAmount = combinedResults.get(row.account) ?? BigInt(0);
      combinedResults.set(row.account, currentAmount + amount);
    });

    // 创建新表并插入数据
    await client1.query(
      "CREATE TABLE combined_accounts (account VARCHAR(255), total_amount TEXT)"
    );
    for (let [account, totalAmount] of combinedResults) {
      await client1.query(
        "INSERT INTO combined_accounts (account, total_amount) VALUES ($1, $2)",
        [account, totalAmount.toString()]
      );
    }

    console.log("Data combined successfully.");
  } catch (error) {
    console.error("An error occurred:", error);
  } finally {
    await client1.end();
    await client2.end();
  }
}

main();
