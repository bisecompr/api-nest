import { Injectable } from '@nestjs/common';
import { DBSQLClient } from '@databricks/sql';
require('dotenv').config()


@Injectable()
export class DatabricksConnectionProvider {
  constructor() {
    this.connect()
  }
  private client = new DBSQLClient();

  async connect() {
    const serverHostname = `${process.env.SERVERHOSTNAME}`
    const httpPath = `${process.env.HTTPPATH}`
    const token = `${process.env.TOKEN}`

    await this.client.connect({ host: serverHostname, path: httpPath, token });
    console.log('Connected to Databricks.');
  }

  getClient() {
    return this.client;
  }

  async executeQuery<T = any>(sql: string): Promise<T[]> {
    const client = this.getClient();

    if (!client) {
      throw new Error('Databricks client is not connected.');
    }

    const session = await client.openSession();
    const statement = await session.executeStatement(sql);
    const rows = await statement.fetchAll();

    await statement.close();
    await session.close();

    return rows as T[];
  }
}
