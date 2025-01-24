import { Injectable } from '@nestjs/common';
import { DBSQLClient } from '@databricks/sql';

@Injectable()
export class DatabricksConnectionProvider {
  constructor() {
    this.connect()
  }
  private client = new DBSQLClient();

  async connect() {
    const serverHostname = 'adb-780571104098995.15.azuredatabricks.net'
    const httpPath = '/sql/1.0/warehouses/2da57dd8e33c5731'
    const token = 'dapi77e18782891d10dcdcb7d595e5117771'

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
