import { Injectable } from '@nestjs/common';
import { DBSQLClient } from '@databricks/sql';

@Injectable()
export class DatabricksService {
  private client = new DBSQLClient();

  constructor() {
    const serverHostname = 'adb-780571104098995.15.azuredatabricks.net'
    const httpPath = '/sql/1.0/warehouses/2da57dd8e33c5731'
    const token = 'dapi77e18782891d10dcdcb7d595e5117771'

    this.client.connect({ host: serverHostname, path: httpPath, token });
  }

  async executeQuery(sql: string): Promise<any[]> {
    const session = await this.client.openSession();
    const statement = await session.executeStatement(sql);
    const rows = await statement.fetchAll();

    await statement.close();
    await session.close();

    return rows;
  }
}
