import { Injectable } from '@nestjs/common';
import { DBSQLClient } from '@databricks/sql';
import { DatabricksConnectionProvider } from './databricks.provider';

@Injectable()
export class DatabricksService {
  constructor(private readonly connectionProvider: DatabricksConnectionProvider) { }

  async executeQuery<T = any>(sql: string): Promise<T[]> {
    const client = this.connectionProvider.getClient();
    const session = await client.openSession();
    const statement = await session.executeStatement(sql);
    const rows = await statement.fetchAll();

    await statement.close();
    await session.close();

    return rows as T[];
  }
}
