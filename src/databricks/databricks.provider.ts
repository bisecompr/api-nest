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
}
