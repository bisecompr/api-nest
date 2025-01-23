import { Injectable } from '@nestjs/common';
import { DatabricksConnectionProvider } from './databricks.provider';

@Injectable()
export class DatabricksService {
  constructor(private readonly connectionProvider: DatabricksConnectionProvider) { }

  private async executeQuery<T = any>(sql: string): Promise<T[]> {
    const client = this.connectionProvider.getClient();

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

  async getCampaigns() {
    const string = 'SELECT DISTINCT NOME_INTERNO_CAMPANHA FROM main.plataformas.plataformas_dia'
    return this.executeQuery(string)
  }

  async getAllMetrics() {
    const string = `
    select
      Nome_Interno_Campanha,
      sum(metrics_cost) as spend,
      sum(metrics_impressions) as impressions,
      sum(metrics_clicks) as clicks,
      sum(metrics_engagement) as engagement
    from main.plataformas.plataformas_dia
    group by Nome_Interno_Campanha
    order by impressions desc`

    return this.executeQuery(string)
  }

  async getCampaignMetrics(campaignName: string, startDate?: string, endDate?: string) {
    if (startDate || endDate) {
      const whereClause = `WHERE date > ${startDate} AND date < ${endDate}`
    }
    const today = new Date
  }
}
