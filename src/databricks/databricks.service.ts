import { Injectable } from '@nestjs/common';
import { DatabricksConnectionProvider } from './databricks.provider';
import knex from 'knex';

@Injectable()
export class DatabricksService {
  constructor(private readonly connectionProvider: DatabricksConnectionProvider) { }
  private knexBuilder = knex({
    client: 'pg',
    useNullAsDefault: true,
    wrapIdentifier: (value, origImpl) => `\`${value}\``
  })

  async getMetrics(campaignName?: string, startDate?: string, endDate?: string) {
    let baseQuery = this.knexBuilder
      .select('Nome_Interno_Campanha')
      .sum({ spend: 'metrics_cost' })
      .sum({ impressions: 'metrics_impressions' })
      .sum({ clicks: 'metrics_clicks' })
      .sum({ engagement: 'metrics_engagement' })
      .sum({ 'video_views_total': 'metrics_video_views' })
      .sum({ 'video_view_25%': 'metrics_video_25p' })
      .sum({ 'video_view_50%': 'metrics_video_50p' })
      .sum({ 'video_view_75%': 'metrics_video_75p' })
      .sum({ 'video_view_100%': 'metrics_video_100p' })
      .sum({ conversions_value: 'metrics_conversions_value' })
      .from('main.plataformas.plataformas_dia')
      .groupBy('Nome_Interno_Campanha')
      .orderBy('impressions', 'desc')

    if (startDate && endDate) {
      baseQuery = baseQuery
        .whereBetween('date', [startDate, endDate])
    } else {
      const today = new Date()
      let lastWeek = new Date()
      lastWeek.setDate(today.getDate() - 7)
      const todayFormatted = today.toISOString().split('T')[0];
      const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

      baseQuery = baseQuery
        .whereBetween('date', [lastWeekFormatted, todayFormatted])
    }
    if (campaignName) {
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }

    const queryString = baseQuery.toString()
    console.log(queryString)
    return this.connectionProvider.executeQuery(queryString)
  }

  // async getCampaigns() {
  //   const query = this.knexBuilder
  //     .select('NOME_INTERNO_CAMPANHA')
  //     .distinct()
  //     .from('main.plataformas.plataformas_dia')
  //     .toString();
  //   console.log(query)

  //   return this.connectionProvider.executeQuery(query);
  // }

  // async getCampaignMetrics(campaignName: string, startDate?: string, endDate?: string) {
  //   let query = this.knexBuilder
  //     .select('*')
  //     .from('main.plataformas.plataformas_dia')
  //     .where('Nome_Interno_Campanha', campaignName)
  //     .groupBy('Nome_Interno_Campanha')

  //   if (startDate && endDate) {
  //     query = query.whereBetween('date', [startDate, endDate]);
  //   }

  //   return this.connectionProvider.executeQuery(query.toString());
  // }
}
